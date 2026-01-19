package main

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

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

type eventDispatcher struct {
	events  chan event.GenericEvent
	filters map[schema.GroupVersionKind][]eventFilter
}

func newEventDispatcher(events chan event.GenericEvent, filters map[schema.GroupVersionKind][]eventFilter) *eventDispatcher {
	return &eventDispatcher{
		events:  events,
		filters: copyFilters(filters),
	}
}

type eventFilter func(obj client.Object) bool

func (d *eventDispatcher) Handle(gvk schema.GroupVersionKind, cObj client.Object) {
	//cobj, ok := clientObjectFromEvent(obj)
	//if !ok {
	//	return
	//}
	filters := d.filters[gvk]
	for _, filter := range filters {
		if filter(cObj) {
			d.events <- event.GenericEvent{Object: cObj}
			return
		}
	}
}

func copyFilters(filters map[schema.GroupVersionKind][]eventFilter) map[schema.GroupVersionKind][]eventFilter {
	if len(filters) == 0 {
		return map[schema.GroupVersionKind][]eventFilter{}
	}
	copied := make(map[schema.GroupVersionKind][]eventFilter, len(filters))
	for gvk, list := range filters {
		filterList := make([]eventFilter, len(list))
		copy(filterList, list)
		copied[gvk] = filterList
	}
	return copied
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
