package parquet

import (
	"github.com/segmentio/parquet/internal/debug"
)

// StructBuilder implements the RowBuilder interface.
// See NewStructBuilder for details.
type StructBuilder struct {
}

// NewStructBuilder creates a new StructBuilder targeting v.
// This method will generate a plan to efficiently decode and construct the
// type of the data pointed to by v.
//
// The row is constructed on the value pointed by v. The builder will overwrite
// any fields present in the interface, as encountered during the decoding.
// Fields not encountered during the decoding won't be modified.
//
// StructBuilder uses the following mapping between Go values and Parquet
// schema:
//
//   BOOLEAN -> bool
//   INT32 (no annotation) -> int32
//   INT64 (no annotation) -> int64
//     Bit width 8, 16, 32, 64 and sign true/false annotations map to their
//     respective Go types.
//   INT96 -> not supported
//   FLOAT -> float32
//   DOUBLE -> float64
//   DECIMAL -> not supported
//   BYTE_ARRAY -> []byte
//   STRING -> string
//   ENUM -> string
//   UUID -> TODO []byte (of length 16)
//   DATE -> TODO time.Time
//   TIME -> TODO time.Time
//   TIMESTAMP -> TODO time.Time
//   INTERVAL -> not supported
//   JSON -> []byte
//   BSON -> []byte
//   LIST -> []T
//   MAP -> map[K]V
//   NULL -> not supported
//
// Optionals are decoded as pointers of the type they wrap.
//
// This builder follows parquet-format's Logical Types specification. Read
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
// for details about the semantics of the conversions.
//
// By default, the builder maps a field's name to its snake_case equivalent.
// You can overwrite this behavior on a per-field basis using the
// `parquet:"..."` field annotation.
//
// Only exported fields are considered.
func NewStructBuilder(s *Schema) *StructBuilder {
	panic("implement me")
}

func (sb *StructBuilder) Begin() {
	debug.Format("StructBuilder - Begin")
}

func (sb *StructBuilder) Primitive(s *Schema, d Decoder) error {
	panic("implement me")
}

func (sb *StructBuilder) PrimitiveNil(s *Schema) error {
	panic("implement me")
}

func (sb *StructBuilder) GroupBegin(s *Schema) {
	panic("implement me")
}

func (sb *StructBuilder) GroupEnd(node *Schema) {
	panic("implement me")
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
