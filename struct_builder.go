package parquet

import (
	"fmt"
	"reflect"

	"github.com/segmentio/parquet/internal/debug"
)

// StructBuilder implements the RowBuilder interface.
// See NewStructBuilder for details.
type StructBuilder struct {
	index  map[*Schema]*blueprint
	target reflect.Value
	stack  valueStack
}

func (sb *StructBuilder) Begin() {
	debug.Format("StructBuilder - Begin")
	sb.stack.push(sb.target.Elem())
}

func (sb *StructBuilder) Primitive(s *Schema, d Decoder) error {
	bp, ok := sb.index[s]
	if !ok {
		panic("entry in schema not found")
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
	v := reflect.Zero(bp.t)
	bp.set(&sb.stack, v)
	sb.stack.push(v)
}

func (sb *StructBuilder) GroupEnd(node *Schema) {
	sb.stack.pop()
}

func (sb *StructBuilder) RepeatedBegin(s *Schema) {
	bp := sb.index[s]
	v := reflect.Zero(bp.t)
	bp.set(&sb.stack, v)
	sb.stack.push(v)
}

func (sb *StructBuilder) RepeatedEnd(node *Schema) {
	sb.stack.pop()
}

func (sb *StructBuilder) KVBegin(node *Schema) {
	panic("implement me")
}

func (sb *StructBuilder) KVEnd(node *Schema) {
	panic("implement me")
}

func (sb *StructBuilder) End() {
	debug.Format("StructBuilder - End")
}

func (sb *StructBuilder) To(v interface{}) *StructBuilder {
	sb.target = reflect.ValueOf(v)
	if sb.target.Kind() != reflect.Ptr {
		panic(fmt.Errorf("need to target a pointer, not %s", sb.target.Kind()))
	}
	return sb
}
