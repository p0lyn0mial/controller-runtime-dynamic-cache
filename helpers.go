package main

import (
	"fmt"

	libraryinputresources "github.com/openshift/multi-operator-manager/pkg/library/libraryinputresources"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

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
