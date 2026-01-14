package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type DynamicReconciler struct {
	Log                logr.Logger
	Mapper             meta.RESTMapper
	Scheme             *runtime.Scheme
	Cache              cache.Cache
	InputResourcesInit *inputResourceInitializer
}

func (r *DynamicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	time.Sleep(time.Second)
	log := r.Log.WithValues("operator", req.Name)
	log.Info("observed operator")
	if r.Mapper == nil {
		return ctrl.Result{}, fmt.Errorf("restmapper is not configured")
	}
	if r.Cache == nil {
		return ctrl.Result{}, fmt.Errorf("cache is not configured")
	}
	if r.Scheme == nil {
		return ctrl.Result{}, fmt.Errorf("scheme is not configured")
	}
	if r.InputResourcesInit == nil {
		return ctrl.Result{}, fmt.Errorf("input resources initializer is not configured")
	}

	inputResources, err := r.InputResourcesInit.discoverInputResources()
	if err != nil {
		return ctrl.Result{}, err
	}

	for _, resources := range inputResources {
		for _, def := range resources {
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

			typedObj, err := r.Scheme.New(gvk)
			if err != nil {
				return ctrl.Result{}, err
			}
			typedClientObj, ok := typedObj.(client.Object)
			if !ok {
				return ctrl.Result{}, fmt.Errorf("type %T does not implement client.Object", typedObj)
			}
			key := client.ObjectKey{Namespace: def.Namespace, Name: def.Name}
			if err := r.Cache.Get(ctx, key, typedClientObj); err != nil {
				if apierrors.IsNotFound(err) {
					log.Info("resource not found", "gvk", gvk.String(), "name", key)
					continue
				}
				return ctrl.Result{}, err
			}

			unstructuredMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typedObj)
			if err != nil {
				return ctrl.Result{}, err
			}
			obj := &unstructured.Unstructured{Object: unstructuredMap}
			obj.SetGroupVersionKind(gvk)

			log.Info(
				"resource from cache",
				"gvk", gvk.String(),
				"name", key,
				"uid", obj.GetUID(),
				"resourceVersion", obj.GetResourceVersion(),
			)
		}
	}
	return ctrl.Result{}, nil
}

func (r *DynamicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Scheme == nil {
		return fmt.Errorf("scheme is not configured")
	}
	if r.InputResourcesInit == nil {
		return fmt.Errorf("input resources initializer is not configured")
	}
	c, err := controller.New("dynamic-unstructured", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	dispatcher := r.InputResourcesInit.dispatcher
	syncedCh := r.InputResourcesInit.syncedCh
	channelSource := source.Channel(dispatcher.events, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		operatorName := operatorNameFromResource(obj)
		gvk, err := apiutil.GVKForObject(obj, r.Scheme)
		if err != nil {
			gvk = obj.GetObjectKind().GroupVersionKind()
		}
		_ = gvk
		return []reconcile.Request{requestForOperator(operatorName, obj)}
	}))
	if err := c.Watch(&syncingChannelSource{source: channelSource, synced: syncedCh}); err != nil {
		return err
	}

	return nil
}
