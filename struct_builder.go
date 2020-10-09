package parquet

import (
	"fmt"
	"reflect"
)

// StructBuilder implements the RowBuilder interface.
// It executes the plan constructed by StructPlanner.
type StructBuilder struct {
	index  map[*Schema]*blueprint
	target reflect.Value
	stack  valueStack
}

func (sb *StructBuilder) Begin() {
	sb.stack.push(sb.target.Elem())
}

func (sb *StructBuilder) Primitive(s *Schema, d Decoder) error {
	bp, ok := sb.index[s]
	if !ok {
		panic("entry in s not found")
	}
	v, err := bp.read(d)
	if err != nil {
		return err
	}
	bp.set(&sb.stack, v)
	return nil
}

func (sb *StructBuilder) PrimitiveNil(s *Schema) error {
	panic("implement me")
}

func (sb *StructBuilder) GroupBegin(s *Schema) {
	if s.Root {
		// TODO: maybe support top being nil and create the top level structure?
		return
	}
	bp := sb.index[s]
	v := bp.create()
	n := bp.set(&sb.stack, v)
	sb.stack.push(n)
}

func (sb *StructBuilder) GroupEnd(node *Schema) {
	sb.stack.pop()
}

func (sb *StructBuilder) RepeatedBegin(s *Schema) {
	bp := sb.index[s]
	v := bp.create()
	bp.set(&sb.stack, v)
	sb.stack.push(v)
}

func (sb *StructBuilder) RepeatedEnd(node *Schema) {
	sb.stack.pop()
}

func (sb *StructBuilder) KVBegin(node *Schema) {
	// nothing to do
}

func (sb *StructBuilder) KVEnd(node *Schema) {
	// nothing to do
}

func (sb *StructBuilder) End() {
	// nothing to do
}

func (sb *StructBuilder) To(v interface{}) *StructBuilder {
	sb.target = reflect.ValueOf(v)
	if sb.target.Kind() != reflect.Ptr {
		panic(fmt.Errorf("need to target a pointer, not %s", sb.target.Kind()))
	}
	return sb
}
