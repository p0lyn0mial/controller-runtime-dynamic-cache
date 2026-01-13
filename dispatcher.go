package main

import (
	"context"

	libraryinputresources "github.com/openshift/multi-operator-manager/pkg/library/libraryinputresources"
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
	events chan event.GenericEvent
}

func newEventDispatcher(bufferSize int) *eventDispatcher {
	return &eventDispatcher{events: make(chan event.GenericEvent, bufferSize)}
}

func (d *eventDispatcher) Handle(def libraryinputresources.ExactResourceID, obj interface{}) {
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
	d.events <- event.GenericEvent{Object: cobj}
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
