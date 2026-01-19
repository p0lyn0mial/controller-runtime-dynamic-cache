package main

import (
	"context"
	"fmt"
	"time"

	libraryinputresources "github.com/openshift/multi-operator-manager/pkg/library/libraryinputresources"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

type inputResourceInitializer struct {
	managementClusterRESTMapper meta.RESTMapper
	managementClusterCache      cache.Cache
	dispatcher                  *eventDispatcher
	events                      chan event.GenericEvent
	inputResources              map[string]*libraryinputresources.InputResources
	syncedCh                    chan struct{}
}

func newInputResourceInitializer(mgmtClusterRESTMapper meta.RESTMapper, mgmtClusterCache cache.Cache, bufferSize int) *inputResourceInitializer {
	return &inputResourceInitializer{
		managementClusterRESTMapper: mgmtClusterRESTMapper,
		managementClusterCache:      mgmtClusterCache,
		events:                      make(chan event.GenericEvent, bufferSize),
		syncedCh:                    make(chan struct{}),
	}
}

func (r *inputResourceInitializer) Start(ctx context.Context) error {
	inputResources, err := r.discoverInputResources()
	if err != nil {
		return err
	}
	if err = r.checkSupportedInputResources(inputResources); err != nil {
		return err
	}
	filters, err := buildInputResourceFilters(inputResources, r.managementClusterRESTMapper)
	if err != nil {
		return err
	}
	r.dispatcher = newEventDispatcher(r.events, filters)
	r.inputResources = inputResources
	return r.startAndWaitForInformersFor(ctx, inputResources)
}

func (r *inputResourceInitializer) startAndWaitForInformersFor(ctx context.Context, inputResources map[string]*libraryinputresources.InputResources) error {
	ctrl.Log.WithName("dynamic-unstructured").Info("syncing the input resources")
	time.Sleep(5 * time.Second)
	for operator, resources := range inputResources {
		registeredGVK := sets.NewString()
		for _, exactResource := range resources.ApplyConfigurationResources.ExactResources {
			gvr := schema.GroupVersionResource{
				Group:    exactResource.Group,
				Version:  exactResource.Version,
				Resource: exactResource.Resource,
			}
			gvk, err := r.managementClusterRESTMapper.KindFor(gvr)
			if err != nil {
				return fmt.Errorf("unable to find Kind for %#v, for %s operator, err: %w", exactResource, operator, err)
			}
			gvkStr := gvk.String()
			if registeredGVK.Has(gvkStr) {
				ctrl.Log.WithName("dynamic-unstructured").Info("gvk already registered", "gvk", gvkStr)
				continue
			}

			gvkForHandler := gvk
			informer, err := r.managementClusterCache.GetInformerForKind(ctx, gvkForHandler, cache.BlockUntilSynced(true))
			if err != nil {
				return err
			}
			_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					cObj, ok := obj.(client.Object)
					if !ok {
						utilruntime.HandleError(fmt.Errorf("added object %+v is not client.Object", obj))
						return
					}
					r.dispatcher.Handle(gvkForHandler, cObj)
				},
				UpdateFunc: func(_, newObj interface{}) {
					cObj, ok := newObj.(client.Object)
					if !ok {
						utilruntime.HandleError(fmt.Errorf("updated object %+v is not client.Object", newObj))
						return
					}
					r.dispatcher.Handle(gvkForHandler, cObj)
				},
				DeleteFunc: func(obj interface{}) {
					if cObj, ok := obj.(client.Object); ok {
						r.dispatcher.Handle(gvkForHandler, cObj)
						return
					}
					tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown)
					if ok {
						cObj, ok := tombstone.Obj.(client.Object)
						if ok {
							r.dispatcher.Handle(gvkForHandler, cObj)
							return
						}
					}
					utilruntime.HandleError(fmt.Errorf("deleted object %+v is not client.Object", obj))
				},
			})
			if err != nil {
				return err
			}
			registeredGVK.Insert(gvkStr)
		}
	}
	if !r.managementClusterCache.WaitForCacheSync(ctx) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("caches did not sync")
	}
	close(r.syncedCh)
	return nil
}

func (r *inputResourceInitializer) discoverInputResources() (map[string]*libraryinputresources.InputResources, error) {
	return map[string]*libraryinputresources.InputResources{
		"cluster-authentication-operator": {
			ApplyConfigurationResources: libraryinputresources.ResourceList{
				ExactResources: []libraryinputresources.ExactResourceID{
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
							Resource: "secrets",
						},
						Namespace: "foo",
						Name:      "bar",
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
			},
		},
	}, nil
}

func (r *inputResourceInitializer) checkSupportedInputResources(_ map[string]*libraryinputresources.InputResources) error {
	return nil
}

func buildInputResourceFilters(inputResources map[string]*libraryinputresources.InputResources, mapper meta.RESTMapper) (map[schema.GroupVersionKind][]eventFilter, error) {
	filters := make(map[schema.GroupVersionKind][]eventFilter)
	for operator, resources := range inputResources {
		for _, exactResource := range resources.ApplyConfigurationResources.ExactResources {
			gvr := schema.GroupVersionResource{
				Group:    exactResource.Group,
				Version:  exactResource.Version,
				Resource: exactResource.Resource,
			}
			gvk, err := mapper.KindFor(gvr)
			if err != nil {
				return nil, fmt.Errorf("unable to find Kind for %#v, for %s operator, err: %w", exactResource, operator, err)
			}
			filters[gvk] = append(filters[gvk], exactResourceFilter(exactResource))
		}
	}
	return filters, nil
}

func exactResourceFilter(def libraryinputresources.ExactResourceID) eventFilter {
	return func(obj client.Object) bool {
		if def.Namespace != "" && obj.GetNamespace() != def.Namespace {
			return false
		}
		if def.Name != "" && obj.GetName() != def.Name {
			return false
		}
		return true
	}
}
