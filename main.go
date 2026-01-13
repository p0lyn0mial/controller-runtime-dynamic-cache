package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-logr/logr"
	libraryinputresources "github.com/openshift/multi-operator-manager/pkg/library/libraryinputresources"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var inputResources = []libraryinputresources.ExactResourceID{
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "configmaps",
		},
		Namespace: "kube-system",
		Name:      "kube-root-ca.crt",
	},
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "secrets",
		},
		Namespace: "kube-system",
		Name:      "bootstrap-token-abcdef",
	},
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "nodes",
		},
		Name: "kind-control-plane",
	},
}

type DynamicReconciler struct {
	client.Client
	Log    logr.Logger
	Mapper meta.RESTMapper
}

func (r *DynamicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("operator", req.Name)
	log.Info("observed operator")
	if r.Mapper == nil {
		return ctrl.Result{}, fmt.Errorf("restmapper is not configured")
	}

	for _, def := range inputResources {
		id := def.InputResourceTypeIdentifier
		if def.Name == "" {
			log.Info("skipping resource without name", "group", id.Group, "version", id.Version, "resource", id.Resource)
			continue
		}

		gvr := schema.GroupVersionResource{Group: id.Group, Version: id.Version, Resource: id.Resource}
		gvk, err := r.Mapper.KindFor(gvr)
		if err != nil {
			return ctrl.Result{}, err
		}

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		key := client.ObjectKey{Namespace: def.Namespace, Name: def.Name}
		if err := r.Get(ctx, key, obj); err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("resource not found", "gvk", gvk.String(), "name", key)
				continue
			}
			return ctrl.Result{}, err
		}

		log.Info(
			"resource from cache",
			"gvk", gvk.String(),
			"name", key,
			"uid", obj.GetUID(),
			"resourceVersion", obj.GetResourceVersion(),
		)
	}
	return ctrl.Result{}, nil
}

func main() {
	config, err := parseConfiguration(flag.CommandLine, os.Args[1:])
	if err != nil {
		panic(err)
	}
	logger, err := initCustomZapLogger(config.LogLevel, config.LogEncoder)
	if err != nil {
		panic(err)
	}
	logrLogger := zapr.NewLogger(logger)
	ctrl.SetLogger(logrLogger.WithName("ctrl"))
	klog.SetLogger(logrLogger.WithName("klog"))

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: server.Options{BindAddress: "0"},
	})
	if err != nil {
		os.Exit(1)
	}

	reconciler := &DynamicReconciler{
		Client: mgr.GetClient(),
		Log:    ctrl.Log.WithName("dynamic-unstructured"),
		Mapper: mgr.GetRESTMapper(),
	}

	c, err := controller.New("dynamic-unstructured", mgr, controller.Options{Reconciler: reconciler})
	if err != nil {
		os.Exit(1)
	}

	events := make(chan event.GenericEvent, 1024)
	syncedCh := make(chan struct{})
	channelSource := source.Channel(events, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		operatorName := operatorNameFromResource(obj)
		gvk, err := apiutil.GVKForObject(obj, scheme)
		if err != nil {
			gvk = obj.GetObjectKind().GroupVersionKind()
		}
		_ = gvk
		//fmt.Printf("enqueue operator=%s from %s %s/%s\n", operatorName, gvk.String(), obj.GetNamespace(), obj.GetName())
		return []reconcile.Request{requestForOperator(operatorName, obj)}
	}))
	if err := c.Watch(&syncingChannelSource{source: channelSource, synced: syncedCh}); err != nil {
		os.Exit(1)
	}

	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		time.Sleep(5 * time.Second)
		for _, def := range inputResources {
			def := def
			_, obj, err := watchFromExactResourceID(mgr.GetRESTMapper(), scheme, def)
			if err != nil {
				return err
			}
			informer, err := mgr.GetCache().GetInformer(ctx, obj, cache.BlockUntilSynced(true))
			if err != nil {
				return err
			}
			_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					enqueueIfMatch(def, obj, events)
				},
				UpdateFunc: func(_, newObj interface{}) {
					enqueueIfMatch(def, newObj, events)
				},
				DeleteFunc: func(obj interface{}) {
					enqueueIfMatch(def, obj, events)
				},
			})
			if err != nil {
				return err
			}
		}
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("cache did not sync")
		}
		close(syncedCh)
		<-ctx.Done()
		return nil
	})); err != nil {
		os.Exit(1)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}

func requestForOperator(operatorName string, obj client.Object) reconcile.Request {
	name := operatorName
	return reconcile.Request{NamespacedName: client.ObjectKey{Name: name}}
}

func operatorNameFromResource(obj client.Object) string {
	return "example-operator"
}

type syncingChannelSource struct {
	source source.Source
	synced <-chan struct{}
}

var _ source.SyncingSource = (*syncingChannelSource)(nil)

func (s *syncingChannelSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	return s.source.Start(ctx, queue)
}

func (s *syncingChannelSource) WaitForSync(ctx context.Context) error {
	select {
	case <-s.synced:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func enqueueIfMatch(def libraryinputresources.ExactResourceID, obj interface{}, events chan<- event.GenericEvent) {
	cobj, ok := clientObjectFromEvent(obj)
	if !ok {
		return
	}
	if def.Namespace != "" && cobj.GetNamespace() != def.Namespace {
		return
	}
	if def.Name != "" && cobj.GetName() != def.Name {
		return
	}
	events <- event.GenericEvent{Object: cobj}
}

func clientObjectFromEvent(obj interface{}) (client.Object, bool) {
	if cobj, ok := obj.(client.Object); ok {
		return cobj, true
	}
	tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	cobj, ok := tombstone.Obj.(client.Object)
	return cobj, ok
}

func watchFromExactResourceID(mapper meta.RESTMapper, scheme *runtime.Scheme, def libraryinputresources.ExactResourceID) (schema.GroupVersionKind, client.Object, error) {
	id := def.InputResourceTypeIdentifier
	gvr := schema.GroupVersionResource{Group: id.Group, Version: id.Version, Resource: id.Resource}
	gvk, err := mapper.KindFor(gvr)
	if err != nil {
		return schema.GroupVersionKind{}, nil, err
	}

	obj, err := scheme.New(gvk)
	if err != nil {
		return schema.GroupVersionKind{}, nil, err
	}

	cobj, ok := obj.(client.Object)
	if !ok {
		return schema.GroupVersionKind{}, nil, fmt.Errorf("type %T does not implement client.Object", obj)
	}
	return gvk, cobj, nil
}

func initCustomZapLogger(level, encoding string) (*zap.Logger, error) {
	lv := zap.AtomicLevel{}

	i64, err := strconv.ParseInt(level, 10, 8)
	numericLevel := int8(i64)
	if err != nil {
		// not a numeric level, try to unmarshal it as a zapcore.Level ("debug", "info", "warn", "error", "dpanic", "panic", or "fatal")
		err := lv.UnmarshalText([]byte(strings.ToLower(level)))
		if err != nil {
			return nil, err
		}
	} else {
		// numeric level:
		// 1. configure klog if the numeric log level is negative and the absolute value of the negative numeric value represents the klog level.
		// 2. configure the atomic zap level based on the numeric value (5..-9).

		var klogLevel int8 = 0
		if numericLevel < 0 {
			klogLevel = -numericLevel
		}

		klogFlagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		klog.InitFlags(klogFlagSet)
		if err := klogFlagSet.Set("v", strconv.Itoa(int(klogLevel))); err != nil {
			return nil, err
		}

		lv = zap.NewAtomicLevelAt(zapcore.Level(numericLevel))
	}

	enc := strings.ToLower(encoding)
	if enc != "json" && enc != "console" {
		return nil, errors.New("'encoding' parameter can only by either 'json' or 'console'")
	}

	cfg := zap.Config{
		Level:             lv,
		OutputPaths:       []string{"stdout"},
		DisableCaller:     false,
		DisableStacktrace: false,
		Encoding:          enc,
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:  "msg",
			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,
			TimeKey:     "time",
			EncodeTime:  zapcore.ISO8601TimeEncoder,
		},
	}
	return cfg.Build()
}

type Config struct {
	LogLevel   string
	LogEncoder string
}

// ParseConfiguration fills the 'OperatorConfig' from the flags passed to the program
func parseConfiguration(fs *flag.FlagSet, args []string) (Config, error) {
	config := Config{}
	fs.StringVar(&config.LogLevel, "log-level", "info", "Log level. Available values: debug | info | warn | error | dpanic | panic | fatal or a numeric value from -9 to 5, where -9 is the most verbose and 5 is the least verbose.")
	fs.StringVar(&config.LogEncoder, "log-encoder", "json", "Log encoder. Available values: json | console")

	if err := fs.Parse(args); err != nil {
		return Config{}, fmt.Errorf("failed to parse arguments: %w", err)
	}

	return config, nil
}
