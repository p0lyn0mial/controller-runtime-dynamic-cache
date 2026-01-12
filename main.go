package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
	libraryinputresources "github.com/openshift/multi-operator-manager/pkg/library/libraryinputresources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var watchDefs = []libraryinputresources.ExactResourceID{
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "configmaps",
		},
	},
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "secrets",
		},
	},
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "nodes",
		},
	},
}

const (
	emptyGroupToken  = "_"
	gvkPartSeparator = "/"
	gvkNameSeparator = "::"
)

type DynamicReconciler struct {
	client.Client
	Log logr.Logger
}

func (r *DynamicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	gvk, objKey, err := parseRequest(req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}

	log := r.Log.WithValues("gvk", gvk.String(), "name", objKey)

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := r.Get(ctx, objKey, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("observed object", "uid", obj.GetUID(), "resourceVersion", obj.GetResourceVersion())
	return ctrl.Result{}, nil
}

func main() {
	kubeconfig := flag.String("kubeconfig", "", "Path to the kubeconfig file")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	opts := zap.Options{Development: *verbose}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		os.Exit(1)
	}

	cfg := ctrl.GetConfigOrDie()
	if *kubeconfig != "" {
		var err error
		cfg, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			os.Exit(1)
		}
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: server.Options{BindAddress: "0"},
	})
	if err != nil {
		os.Exit(1)
	}

	reconciler := &DynamicReconciler{
		Client: mgr.GetClient(),
		Log:    ctrl.Log.WithName("dynamic-unstructured"),
	}

	c, err := controller.New("dynamic-unstructured", mgr, controller.Options{Reconciler: reconciler})
	if err != nil {
		os.Exit(1)
	}

	for _, def := range watchDefs {
		gvk, obj, err := watchFromExactResourceID(mgr.GetRESTMapper(), scheme, def)
		if err != nil {
			os.Exit(1)
		}
		if err := c.Watch(
			source.Kind(mgr.GetCache(), obj),
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return []reconcile.Request{requestFor(gvk, obj)}
			}),
		); err != nil {
			os.Exit(1)
		}
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}

func encodeGVK(gvk schema.GroupVersionKind) string {
	group := gvk.Group
	if group == "" {
		group = emptyGroupToken
	}
	return group + gvkPartSeparator + gvk.Version + gvkPartSeparator + gvk.Kind
}

func decodeGVK(encoded string) (schema.GroupVersionKind, error) {
	parts := strings.Split(encoded, gvkPartSeparator)
	if len(parts) != 3 {
		return schema.GroupVersionKind{}, fmt.Errorf("invalid gvk encoding: %q", encoded)
	}
	group := parts[0]
	if group == emptyGroupToken {
		group = ""
	}
	return schema.GroupVersionKind{Group: group, Version: parts[1], Kind: parts[2]}, nil
}

func requestFor(gvk schema.GroupVersionKind, obj client.Object) reconcile.Request {
	name := encodeGVK(gvk) + gvkNameSeparator + obj.GetName()
	return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: obj.GetNamespace(), Name: name}}
}

func parseRequest(key client.ObjectKey) (schema.GroupVersionKind, client.ObjectKey, error) {
	parts := strings.SplitN(key.Name, gvkNameSeparator, 2)
	if len(parts) != 2 {
		return schema.GroupVersionKind{}, client.ObjectKey{}, fmt.Errorf("request name missing gvk prefix: %q", key.Name)
	}
	gvk, err := decodeGVK(parts[0])
	if err != nil {
		return schema.GroupVersionKind{}, client.ObjectKey{}, err
	}
	return gvk, client.ObjectKey{Namespace: key.Namespace, Name: parts[1]}, nil
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
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		listGVK := gvk.GroupVersion().WithKind(gvk.Kind + "List")
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
		u := &unstructured.Unstructured{Object: map[string]interface{}{}}
		u.SetGroupVersionKind(gvk)
		return gvk, u, nil
	}

	cobj, ok := obj.(client.Object)
	if !ok {
		return schema.GroupVersionKind{}, nil, fmt.Errorf("type %T does not implement client.Object", obj)
	}
	return gvk, cobj, nil
}
