package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	libraryinputresources "github.com/openshift/multi-operator-manager/pkg/library/libraryinputresources"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
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
	klog.InitFlags(nil)
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	opts := zap.Options{Development: *verbose}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

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

	for _, def := range inputResources {
		def := def
		_, obj, err := watchFromExactResourceID(mgr.GetRESTMapper(), scheme, def)
		if err != nil {
			os.Exit(1)
		}
		var preds []predicate.TypedPredicate[client.Object]
		if def.Namespace != "" || def.Name != "" {
			preds = append(preds, predicate.NewTypedPredicateFuncs(func(obj client.Object) bool {
				if def.Namespace != "" && obj.GetNamespace() != def.Namespace {
					return false
				}
				if def.Name != "" && obj.GetName() != def.Name {
					return false
				}
				return true
			}))
		}
		if err := c.Watch(source.Kind(
			mgr.GetCache(),
			obj,
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				operatorName := operatorNameFromResource(obj)
				gvk, err := apiutil.GVKForObject(obj, scheme)
				if err != nil {
					gvk = obj.GetObjectKind().GroupVersionKind()
				}
				_ = gvk
				//fmt.Printf("enqueue operator=%s from %s %s/%s\n", operatorName, gvk.String(), obj.GetNamespace(), obj.GetName())
				return []reconcile.Request{requestForOperator(operatorName, obj)}
			}),
			preds...,
		)); err != nil {
			os.Exit(1)
		}
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
