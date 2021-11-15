package parquet

import (
	"reflect"
	"sort"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/format"
)

type Group map[string]Node

func (g Group) Type() Type { return groupType{} }

func (g Group) Optional() bool { return false }

func (g Group) Repeated() bool { return false }

func (g Group) Required() bool { return true }

func (g Group) NumChildren() int { return len(g) }

func (g Group) ChildNames() []string {
	names := make([]string, 0, len(g))
	for name := range g {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (g Group) ChildByName(name string) Node {
	n, ok := g[name]
	if ok {
		return n
	}
	panic("column not found in parquet group: " + name)
}

func (g Group) Object(value reflect.Value) Object {
	names := g.ChildNames()
	group := &groupObject{
		group:   g,
		names:   names,
		objects: make([]Object, len(names)),
	}

	if value.IsValid() {
		for i, name := range group.names {
			index := reflect.ValueOf(&group.names[i]).Elem()
			group.objects[i] = g[name].Object(value.MapIndex(index))
		}
	} else {
		for i, name := range group.names {
			group.objects[i] = g[name].Object(reflect.Value{})
		}
	}

	return group
}

type groupType struct{}

func (groupType) Kind() Kind                               { panic("cannot call Kind on parquet group type") }
func (groupType) Length() int                              { panic("cannot call Length on parquet group type") }
func (groupType) LogicalType() *format.LogicalType         { return nil }
func (groupType) ConvertedType() *deprecated.ConvertedType { return nil }
func (groupType) NewPageBuffer(int) PageBuffer             { panic("cannot create page buffer for parquet group") }

type groupObject struct {
	group   Group
	names   []string
	objects []Object
}

func (obj *groupObject) Len() int {
	return len(obj.objects)
}

func (obj *groupObject) Index(index int) Object {
	return obj.objects[index]
}

func (obj *groupObject) Value() Value {
	panic("cannot call Value on parquet group object")
}

func (obj *groupObject) Reset(value reflect.Value) {
	if value.IsValid() {
		for i, child := range obj.objects {
			index := reflect.ValueOf(&obj.names[i]).Elem()
			child.Reset(value.MapIndex(index))
		}
	} else {
		for _, child := range obj.objects {
			child.Reset(reflect.Value{})
		}
	}
}
