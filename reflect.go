package parquet

import "github.com/segmentio/parquet/schema"

type Kind int

const (
	Boolean Kind = iota
	Int32
	Int64
	Int96
	Float
	Double
	ByteArray
	FixedLenByteArray
)

type Type interface {
	Kind() Kind
	Length() int
	String() string
}

type schemaElementType struct {
	*schema.SchemaElement
}

func (t schemaElementType) Kind() Kind {
	return Kind(t.Type)
}

func (t schemaElementType) Length() int {
	return int(t.TypeLength)
}

func (t schemaElementType) String() string {
	return t.Type.String()
}
