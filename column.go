package parquet

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

// Column represents a column in a parquet file.
//
// Methods of Column values are safe to call concurrently from multiple
// goroutines.
//
// Column instances satisfy the Node interface.
type Column struct {
	file        *File
	schema      *format.SchemaElement
	order       *format.ColumnOrder
	path        []string
	names       []string
	columns     []*Column
	chunks      []*format.ColumnChunk
	encoding    []encoding.Encoding
	compression []compress.Codec

	depth              int8
	maxRepetitionLevel int8
	maxDefinitionLevel int8
}

// Schema returns the underlying schema element of c.
func (c *Column) Schema() *format.SchemaElement { return c.schema }

// Order returns the underlying column order of c.
func (c *Column) Order() *format.ColumnOrder { return c.order }

// Type returns the type of the column.
//
// The returned value is unspecified if c is not a leaf column.
func (c *Column) Type() Type { return schemaElementType{c.schema} }

// Required returns true if the column is required.
func (c *Column) Required() bool { return schemaRepetitionTypeOf(c.schema) == format.Required }

// Optional returns true if the column is optional.
func (c *Column) Optional() bool { return schemaRepetitionTypeOf(c.schema) == format.Optional }

// Repeated returns true if the column may repeat.
func (c *Column) Repeated() bool { return schemaRepetitionTypeOf(c.schema) == format.Repeated }

// NumChildren returns the number of child columns.
//
// This method contributes to satisfying the Node interface.
func (c *Column) NumChildren() int { return len(c.columns) }

// Children returns the names of child columns.
//
// This method contributes to satisfying the Node interface.
func (c *Column) ChildNames() []string { return c.names }

// ChildByName returns a Node value representing the child column matching the
// name passed as argument.
//
// This method contributes to satisfying the Node interface.
func (c *Column) ChildByName(name string) Node { return c.Column(name) }

// Encoding returns the encodings used by this column.
func (c *Column) Encoding() []encoding.Encoding { return c.encoding }

// Compression returns the compression codecs used by this column.
func (c *Column) Compression() []compress.Codec { return c.compression }

// Path of the column in the parquet schema.
func (c *Column) Path() []string { return c.path }

// Name returns the column name.
func (c *Column) Name() string { return c.schema.Name }

// Columns returns the list of child columns.
//
// The method returns the same slice across multiple calls, the program must
// treat it as a read-only value.
func (c *Column) Columns() []*Column { return c.columns }

// Column returns the child column matching the given name.
func (c *Column) Column(name string) *Column {
	i := sort.Search(len(c.columns), func(i int) bool {
		return c.columns[i].Name() >= name
	})
	if i < len(c.columns) && c.columns[i].Name() == name {
		return c.columns[i]
	}
	return nil
}

// Chunks returns an iterator over the column chunks that compose this column.
func (c *Column) Chunks() *ColumnChunks { return &ColumnChunks{column: c, index: -1} }

// Depth returns the position of the column relative to the root.
func (c *Column) Depth() int { return int(c.depth) }

// MaxRepetitionLevel returns the maximum value of repetition levels on this
// column.
func (c *Column) MaxRepetitionLevel() int { return int(c.maxRepetitionLevel) }

// MaxDefinitionLevel returns the maximum value of definition levels on this
// column.
func (c *Column) MaxDefinitionLevel() int { return int(c.maxDefinitionLevel) }

// String returns a human-redable string representation of the oclumn.
func (c *Column) String() string {
	switch {
	case c.columns != nil:
		return fmt.Sprintf("%s{%s,R=%d,D=%d}",
			join(c.path),
			c.schema.RepetitionType,
			c.maxRepetitionLevel,
			c.maxDefinitionLevel)

	case c.Type().Kind() == FixedLenByteArray:
		return fmt.Sprintf("%s{%s(%d),%s,R=%d,D=%d}",
			join(c.path),
			c.schema.Type,
			c.schema.TypeLength,
			c.schema.RepetitionType,
			c.maxRepetitionLevel,
			c.maxDefinitionLevel)

	default:
		return fmt.Sprintf("%s{%s,%s,R=%d,D=%d}",
			join(c.path),
			c.schema.Type,
			c.schema.RepetitionType,
			c.maxRepetitionLevel,
			c.maxDefinitionLevel)
	}
}

func join(path []string) string {
	return strings.Join(path, ".")
}

func openColumns(file *File) (*Column, error) {
	cl := columnLoader{}

	c, err := cl.open(file, nil)
	if err != nil {
		return nil, err
	}

	// Validate that there aren't extra entries in the row group columns,
	// which would otherwise indicate that there are dangling data pages
	// in the file.
	for index, rowGroup := range file.metadata.RowGroups {
		if cl.rowGroupColumnIndex != len(rowGroup.Columns) {
			return nil, fmt.Errorf("row group at index %d contains %d columns but %d were referenced by the column schemas",
				index, len(rowGroup.Columns), cl.rowGroupColumnIndex)
		}
	}

	return c, setMaxLevels(c, 0, 0, 0)
}

func setMaxLevels(col *Column, depth, repetition, definition int8) error {
	switch schemaRepetitionTypeOf(col.schema) {
	case format.Optional:
		definition++
	case format.Repeated:
		repetition++
		definition++
	}

	col.depth = depth
	col.maxRepetitionLevel = repetition
	col.maxDefinitionLevel = definition
	depth++

	if depth < 0 {
		return fmt.Errorf("cannot represent parquet columns with more than 127 nested levels: %s", join(col.path))
	}
	if repetition < 0 {
		return fmt.Errorf("cannot represent parquet columns with more than 127 repetition levels: %s", join(col.path))
	}
	if definition < 0 {
		return fmt.Errorf("cannot represent parquet columns with more than 127 definition levels: %s", join(col.path))
	}

	for _, c := range col.columns {
		if err := setMaxLevels(c, depth, repetition, definition); err != nil {
			return err
		}
	}

	return nil
}

type columnLoader struct {
	schemaIndex         int
	columnOrderIndex    int
	rowGroupColumnIndex int
}

func (cl *columnLoader) open(file *File, path []string) (*Column, error) {
	c := &Column{
		file:   file,
		schema: &file.metadata.Schema[cl.schemaIndex],
	}
	c.path = append(path[:len(path):len(path)], c.schema.Name)

	cl.schemaIndex++
	numChildren := int(c.schema.NumChildren)

	if numChildren == 0 {
		if cl.columnOrderIndex < len(file.metadata.ColumnOrders) {
			c.order = &file.metadata.ColumnOrders[cl.columnOrderIndex]
			cl.columnOrderIndex++
		}

		c.chunks = make([]*format.ColumnChunk, 0, len(file.metadata.RowGroups))
		for index, rowGroup := range file.metadata.RowGroups {
			if cl.rowGroupColumnIndex >= len(rowGroup.Columns) {
				return nil, fmt.Errorf("row group at index %d does not have enough columns", index)
			}
			c.chunks = append(c.chunks, &rowGroup.Columns[cl.rowGroupColumnIndex])
		}

		c.encoding = make([]encoding.Encoding, 0, len(c.chunks))
		for _, chunk := range c.chunks {
			for _, encoding := range chunk.MetaData.Encoding {
				c.encoding = append(c.encoding, lookupEncoding(encoding))
			}
		}
		sortEncodings(c.encoding)
		c.encoding = dedupeSortedEncodings(c.encoding)

		c.compression = make([]compress.Codec, len(c.chunks))
		for i, chunk := range c.chunks {
			c.compression[i] = lookupCompressionCodec(chunk.MetaData.Codec)
		}
		sortCodecs(c.compression)
		c.compression = dedupeSortedCodecs(c.compression)

		cl.rowGroupColumnIndex++
		return c, nil
	}

	c.names = make([]string, numChildren)
	c.columns = make([]*Column, numChildren)

	for i := range c.columns {
		if cl.schemaIndex >= len(file.metadata.Schema) {
			return nil, fmt.Errorf("column %q has more children than there are schemas in the file: %d > %d",
				c.schema.Name, cl.schemaIndex+1, len(file.metadata.Schema))
		}

		var err error
		c.columns[i], err = cl.open(file, path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.schema.Name, err)
		}
	}

	for i, col := range c.columns {
		c.names[i] = col.Name()
	}

	c.encoding = mergeColumnEncoding(c.columns)
	c.compression = mergeColumnCompression(c.columns)
	sort.Sort(columnsByName{c})
	return c, nil
}

func mergeColumnEncoding(columns []*Column) []encoding.Encoding {
	index := make(map[format.Encoding]encoding.Encoding)
	for _, col := range columns {
		for _, e := range col.encoding {
			index[e.Encoding()] = e
		}
	}
	merged := make([]encoding.Encoding, 0, len(index))
	for _, encoding := range index {
		merged = append(merged, encoding)
	}
	sortEncodings(merged)
	return merged
}

func mergeColumnCompression(columns []*Column) []compress.Codec {
	index := make(map[format.CompressionCodec]compress.Codec)
	for _, col := range columns {
		for _, c := range col.compression {
			index[c.CompressionCodec()] = c
		}
	}
	merged := make([]compress.Codec, 0, len(index))
	for _, codec := range index {
		merged = append(merged, codec)
	}
	sortCodecs(merged)
	return merged
}

type columnsByName struct{ *Column }

func (c columnsByName) Len() int { return len(c.names) }

func (c columnsByName) Less(i, j int) bool { return c.names[i] < c.names[j] }

func (c columnsByName) Swap(i, j int) {
	c.names[i], c.names[j] = c.names[j], c.names[i]
	c.columns[i], c.columns[j] = c.columns[j], c.columns[i]
}

type schemaElementType struct{ *format.SchemaElement }

func (t schemaElementType) Kind() Kind {
	if t.Type != nil {
		return Kind(*t.Type)
	}
	return -1
}

func (t schemaElementType) Length() int {
	if t.TypeLength != nil {
		return int(*t.TypeLength)
	}
	return 0
}

func (t schemaElementType) Less(v1, v2 Value) bool {
	// First try to apply a logical type comparison.
	switch lt := t.LogicalType(); {
	case lt.UTF8 != nil:
		return (*stringType)(lt.UTF8).Less(v1, v2)
	case lt.Map != nil:
		return (*mapType)(lt.Map).Less(v1, v2)
	case lt.List != nil:
		return (*listType)(lt.List).Less(v1, v2)
	case lt.Enum != nil:
		return (*enumType)(lt.Enum).Less(v1, v2)
	case lt.Decimal != nil:
		// TODO:
		// return (*decimalType)(lt.Decimal).Less(v1, v2)
	case lt.Date != nil:
		return (*dateType)(lt.Date).Less(v1, v2)
	case lt.Time != nil:
		return (*timeType)(lt.Time).Less(v1, v2)
	case lt.Timestamp != nil:
		return (*timestampType)(lt.Timestamp).Less(v1, v2)
	case lt.Integer != nil:
		return (*intType)(lt.Integer).Less(v1, v2)
	case lt.Unknown != nil:
		return (*nullType)(lt.Unknown).Less(v1, v2)
	case lt.Json != nil:
		return (*jsonType)(lt.Json).Less(v1, v2)
	case lt.Bson != nil:
		return (*bsonType)(lt.Bson).Less(v1, v2)
	case lt.UUID != nil:
		return (*uuidType)(lt.UUID).Less(v1, v2)
	}

	// If no logical types were found, fallback to doing a basic comparison
	// of the values.
	switch t.Kind() {
	case Boolean:
		return !v1.Boolean() && v2.Boolean()
	case Int32:
		return v1.Int32() < v2.Int32()
	case Int64:
		return v1.Int64() < v2.Int64()
	case Int96:
		return bits.CompareInt96(v1.Int96(), v2.Int96()) < 0
	case Float:
		return v1.Float() < v2.Float()
	case Double:
		return v1.Double() < v2.Double()
	case ByteArray, FixedLenByteArray:
		return bytes.Compare(v1.ByteArray(), v2.ByteArray()) < 0
	default:
		return false
	}
}

func (t schemaElementType) PhyiscalType() *format.Type {
	return t.SchemaElement.Type
}

func (t schemaElementType) LogicalType() *format.LogicalType {
	return t.SchemaElement.LogicalType
}

func (t schemaElementType) ConvertedType() *deprecated.ConvertedType {
	return t.SchemaElement.ConvertedType
}

func (t schemaElementType) NewDictionary(bufferSize int) Dictionary {
	switch t.Kind() {
	case Boolean:
		return newBooleanDictionary(t)
	case Int32:
		return newInt32Dictionary(t, bufferSize)
	case Int64:
		return newInt64Dictionary(t, bufferSize)
	case Int96:
		return newInt96Dictionary(t, bufferSize)
	case Float:
		return newFloatDictionary(t, bufferSize)
	case Double:
		return newDoubleDictionary(t, bufferSize)
	case ByteArray:
		return newByteArrayDictionary(t, bufferSize)
	case FixedLenByteArray:
		return newFixedLenByteArrayDictionary(t, bufferSize)
	default:
		panic("cannot create a page buffer from a schema element of unsupported type")
	}
}

func (t schemaElementType) NewPageBuffer(bufferSize int) PageBuffer {
	switch t.Kind() {
	case Boolean:
		return newBooleanPageBuffer(t, bufferSize)
	case Int32:
		return newInt32PageBuffer(t, bufferSize)
	case Int64:
		return newInt64PageBuffer(t, bufferSize)
	case Int96:
		return newInt96PageBuffer(t, bufferSize)
	case Float:
		return newFloatPageBuffer(t, bufferSize)
	case Double:
		return newDoublePageBuffer(t, bufferSize)
	case ByteArray:
		return newByteArrayPageBuffer(t, bufferSize)
	case FixedLenByteArray:
		return newFixedLenByteArrayPageBuffer(t, bufferSize)
	default:
		panic("cannot create a page buffer from a schema element of unsupported type")
	}
}

func (t schemaElementType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	switch t.Kind() {
	case Boolean:
		return newBooleanPageReader(t, decoder, bufferSize)
	case Int32:
		return newInt32PageReader(t, decoder, bufferSize)
	case Int64:
		return newInt64PageReader(t, decoder, bufferSize)
	case Int96:
		return newInt96PageReader(t, decoder, bufferSize)
	case Float:
		return newFloatPageReader(t, decoder, bufferSize)
	case Double:
		return newDoublePageReader(t, decoder, bufferSize)
	case ByteArray:
		return newByteArrayPageReader(t, decoder, bufferSize)
	case FixedLenByteArray:
		return newFixedLenByteArrayPageReader(t, decoder, bufferSize)
	default:
		panic("cannot create a page buffer from a schema element of unsupported type")
	}
}

func schemaRepetitionTypeOf(s *format.SchemaElement) format.FieldRepetitionType {
	if s.RepetitionType != nil {
		return *s.RepetitionType
	}
	return format.Required
}

var (
	_ Node = (*Column)(nil)
)
