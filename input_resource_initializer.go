package main

import (
	"context"
	"fmt"
	"time"

	libraryinputresources "github.com/openshift/multi-operator-manager/pkg/library/libraryinputresources"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

type inputResourceInitializer struct {
	managementClusterRESTMapper meta.RESTMapper
	managementClusterCache      cache.Cache
	dispatcher                  *eventDispatcher
	syncedCh                    chan struct{}
}

func newInputResourceInitializer(mgmtClusterRESTMapper meta.RESTMapper, mgmtClusterCache cache.Cache, dispatcher *eventDispatcher) *inputResourceInitializer {
	return &inputResourceInitializer{
		managementClusterRESTMapper: mgmtClusterRESTMapper,
		managementClusterCache:      mgmtClusterCache,
		dispatcher:                  dispatcher,
		syncedCh:                    make(chan struct{}),
	}
}

func (r *inputResourceInitializer) Start(ctx context.Context) error {
	ctrl.Log.WithName("dynamic-unstructured").Info("syncing the input resources")
	time.Sleep(5 * time.Second)
	inputResources, err := r.discoverInputResources()
	if err != nil {
		return err
	}
	if err = r.checkSupportedInputResources(inputResources); err != nil {
		return err
	}
	visited := sets.NewString()

	for _, resources := range inputResources {
		for _, def := range resources {
			id := def.InputResourceTypeIdentifier
			gvr := schema.GroupVersionResource{Group: id.Group, Version: id.Version, Resource: id.Resource}
			gvk, err := r.managementClusterRESTMapper.KindFor(gvr)
			if err != nil {
				return err
			}
			gvkStr := gvk.String()
			if visited.Has(gvkStr) {
				ctrl.Log.WithName("dynamic-unstructured").Info("gvk already registered", "gvk", gvkStr)
				continue
			}

			informer, err := r.managementClusterCache.GetInformerForKind(ctx, gvk, cache.BlockUntilSynced(true))
			if err != nil {
				return err
			}
			_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					r.dispatcher.Handle(def, obj)
				},
				UpdateFunc: func(_, newObj interface{}) {
					r.dispatcher.Handle(def, newObj)
				},
				DeleteFunc: func(obj interface{}) {
					r.dispatcher.Handle(def, obj)
				},
			})
			if err != nil {
				return err
			}
			visited.Insert(gvkStr)
		}
	}
	if !r.managementClusterCache.WaitForCacheSync(ctx) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("cache did not sync")
	}
	close(r.syncedCh)
	return nil
}

func (r *inputResourceInitializer) discoverInputResources() (map[string][]libraryinputresources.ExactResourceID, error) {
	return map[string][]libraryinputresources.ExactResourceID{
		"default": {
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
					Resource: "configmaps",
				},
				Namespace: "kube-system",
				Name:      "kubeadm-config",
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
		},
	}, nil
}

func (r *inputResourceInitializer) checkSupportedInputResources(_ map[string][]libraryinputresources.ExactResourceID) error {
	return nil
}
