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
	index   map[*Schema]*blueprint
	target  reflect.Value
	current reflect.Value
}

func (sb *StructBuilder) Begin() {
	debug.Format("StructBuilder - Begin")
	sb.current = sb.target
}

func (sb *StructBuilder) Primitive(s *Schema, d Decoder) error {
	bp, ok := sb.index[s]
	if !ok {
		panic("entry in schema not found")
	}
	f := sb.current.Elem().Field(bp.idx)
	switch s.PhysicalType {
	case pthrift.Type_INT32:
		v, err := d.Int32()
		if err != nil {
			return err
		}
		f.SetInt(int64(v)) // suspicious
	case pthrift.Type_BYTE_ARRAY:
		b, err := d.ByteArray(nil) // alloc
		if err != nil {
			return err
		}
		if s.ConvertedType != nil && *s.ConvertedType == pthrift.ConvertedType_UTF8 {
			f.SetString(string(b))
		} else {
			f.Set(reflect.ValueOf(b))
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
	// when a group begins, it is assumed that the struct has already been initialized.
}

func (sb *StructBuilder) GroupEnd(node *Schema) {
	// not much to do when a group ends
}

func (sb *StructBuilder) RepeatedBegin(s *Schema) {
	panic("implement me")
}

func (sb *StructBuilder) RepeatedEnd(node *Schema) {
	panic("implement me")
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
