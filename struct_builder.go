package parquet

import (
	"fmt"
	"reflect"

	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"

	"github.com/segmentio/parquet/internal/debug"
)

// StructBuilder implements the RowBuilder interface.
// See NewStructBuilder for details.
type StructBuilder struct {
	index  map[*Schema]*blueprint
	target reflect.Value
	stack  []reflect.Value
}

func (sb *StructBuilder) push(v reflect.Value) {
	sb.stack = append(sb.stack, v)
}

func (sb *StructBuilder) pop() {
	sb.stack = sb.stack[:len(sb.stack)-1]
}

func (sb *StructBuilder) current() reflect.Value {
	return sb.stack[len(sb.stack)-1]
}

func (sb *StructBuilder) Begin() {
	debug.Format("StructBuilder - Begin")
	sb.push(sb.target)
}

func (sb *StructBuilder) Primitive(s *Schema, d Decoder) error {
	bp, ok := sb.index[s]
	if !ok {
		panic("entry in schema not found")
	}
	// TODO: all that info is known at planning time. Move there.
	switch s.PhysicalType {
	case pthrift.Type_INT32:
		v, err := d.Int32()
		if err != nil {
			return err
		}
		if sb.current().Kind() == reflect.Slice {
			slice := sb.current()
			sb.pop()
			slice = reflect.Append(slice, reflect.ValueOf(v))

			// does not work in all cases
			sb.current().Elem().Field(bp.parent.idx).Set(slice)

			sb.push(slice)
		} else if sb.current().Elem().Kind() == reflect.Struct {
			f := sb.current().Elem().Field(bp.idx)
			f.SetInt(int64(v)) // suspicious
		}
	case pthrift.Type_BYTE_ARRAY:
		b, err := d.ByteArray(nil) // alloc
		if err != nil {
			return err
		}

		if sb.current().Elem().Kind() == reflect.Struct {
			f := sb.current().Elem().Field(bp.idx)
			if s.ConvertedType != nil && *s.ConvertedType == pthrift.ConvertedType_UTF8 {
				f.SetString(string(b))
			} else {
				f.Set(reflect.ValueOf(b))
			}
		}
	default:
		panic(fmt.Errorf("unsupported physical type: %s", s.PhysicalType.String()))
	}
	return nil
}

func (sb *StructBuilder) PrimitiveNil(s *Schema) error {
	panic("implement me")
}

func (sb *StructBuilder) GroupBegin(s *Schema) {
	if s.Root {
		// TODO: maybe support current being nil and create the top level structure?
		return
	}

	// only works if the group is part of a struct
	bp := sb.index[s]
	f := sb.current().Elem().Field(bp.idx)
	f.Set(reflect.Zero(bp.t))
	sb.push(f.Addr())
}

func (sb *StructBuilder) GroupEnd(node *Schema) {
	sb.pop()
}

func (sb *StructBuilder) RepeatedBegin(s *Schema) {
	bp := sb.index[s]
	// only works if the repeated group is part of a struct
	f := sb.current().Elem().Field(bp.idx)
	f.Set(reflect.Zero(bp.t))
	sb.push(f)
}

func (sb *StructBuilder) RepeatedEnd(node *Schema) {
	sb.pop()
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
