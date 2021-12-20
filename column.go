package parquet

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

// Column represents a column in a parquet file.
//
// Methods of Column values are safe to call concurrently from multiple
// goroutines.
//
// Column instances satisfy the Node interface.
type Column struct {
	typ         Type
	file        *File
	schema      *format.SchemaElement
	order       *format.ColumnOrder
	path        []string
	names       []string
	columns     []*Column
	chunks      []*format.ColumnChunk
	columnIndex []*ColumnIndex
	offsetIndex []*OffsetIndex
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
func (c *Column) Type() Type { return c.typ }

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

// ChildNames returns the names of child columns.
//
// This method contributes to satisfying the Node interface.
func (c *Column) ChildNames() []string { return c.names }

// ChildByName returns a Node value representing the child column matching the
// name passed as argument.
//
// This method contributes to satisfying the Node interface.
func (c *Column) ChildByName(name string) Node {
	if child := c.Column(name); child != nil {
		return child
	}
	return nil
}

// ChildByIndex returns a Node value representing the child column at the given
// index.
//
// This method contributes to satisfying the IndexedNode interface.
func (c *Column) ChildByIndex(index int) Node {
	if index >= 0 && index < len(c.columns) {
		return c.columns[index]
	}
	return nil
}

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
func (c *Column) MaxRepetitionLevel() int8 { return c.maxRepetitionLevel }

// MaxDefinitionLevel returns the maximum value of definition levels on this
// column.
func (c *Column) MaxDefinitionLevel() int8 { return c.maxDefinitionLevel }

// ValueByName returns the sub-value with the given name in base.
func (c *Column) ValueByName(base reflect.Value, name string) reflect.Value {
	if len(c.columns) == 0 { // leaf?
		panic("cannot call ValueByName on leaf column")
	}
	return base.MapIndex(reflect.ValueOf(name))
}

// ValueByIndex returns the sub-value in base for the child column at the given
// index.
func (c *Column) ValueByIndex(base reflect.Value, index int) reflect.Value {
	return c.ValueByName(base, c.columns[index].Name())
}

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

func (c *Column) forEachLeaf(do func(*Column)) {
	if len(c.columns) == 0 {
		do(c)
	} else {
		for _, child := range c.columns {
			child.forEachLeaf(do)
		}
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
		c.typ = schemaElementTypeOf(c.schema)

		if cl.columnOrderIndex < len(file.metadata.ColumnOrders) {
			c.order = &file.metadata.ColumnOrders[cl.columnOrderIndex]
			cl.columnOrderIndex++
		}

		rowGroups := file.metadata.RowGroups
		rowGroupColumnIndex := cl.rowGroupColumnIndex
		cl.rowGroupColumnIndex++

		c.chunks = make([]*format.ColumnChunk, 0, len(rowGroups))
		c.columnIndex = make([]*ColumnIndex, 0, len(rowGroups))
		c.offsetIndex = make([]*OffsetIndex, 0, len(rowGroups))

		for i, rowGroup := range rowGroups {
			if rowGroupColumnIndex >= len(rowGroup.Columns) {
				return nil, fmt.Errorf("row group at index %d does not have enough columns", i)
			}
			c.chunks = append(c.chunks, &rowGroup.Columns[rowGroupColumnIndex])
		}

		if len(file.columnIndexes) > 0 {
			for i := range rowGroups {
				if rowGroupColumnIndex >= len(file.columnIndexes) {
					return nil, fmt.Errorf("row group at index %d does not have enough column index pages", i)
				}
				c.columnIndex = append(c.columnIndex, &file.columnIndexes[rowGroupColumnIndex])
			}
		}

		if len(file.offsetIndexes) > 0 {
			for i := range rowGroups {
				if rowGroupColumnIndex >= len(file.offsetIndexes) {
					return nil, fmt.Errorf("row group at index %d does not have enough offset index pages", i)
				}
				c.offsetIndex = append(c.offsetIndex, &file.offsetIndexes[rowGroupColumnIndex])
			}
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
		return c, nil
	}

	c.typ = &groupType{}
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

func schemaElementTypeOf(s *format.SchemaElement) Type {
	if lt := s.LogicalType; lt != nil {
		// A logical type exists, the Type interface implementations in this
		// package are all based on the logical parquet types declared in the
		// format sub-package so we can return them directly via a pointer type
		// conversion.
		switch {
		case lt.UTF8 != nil:
			return (*stringType)(lt.UTF8)
		case lt.Map != nil:
			return (*mapType)(lt.Map)
		case lt.List != nil:
			return (*listType)(lt.List)
		case lt.Enum != nil:
			return (*enumType)(lt.Enum)
		case lt.Decimal != nil:
			// TODO:
			// return (*decimalType)(lt.Decimal)
		case lt.Date != nil:
			return (*dateType)(lt.Date)
		case lt.Time != nil:
			return (*timeType)(lt.Time)
		case lt.Timestamp != nil:
			return (*timestampType)(lt.Timestamp)
		case lt.Integer != nil:
			return (*intType)(lt.Integer)
		case lt.Unknown != nil:
			return (*nullType)(lt.Unknown)
		case lt.Json != nil:
			return (*jsonType)(lt.Json)
		case lt.Bson != nil:
			return (*bsonType)(lt.Bson)
		case lt.UUID != nil:
			return (*uuidType)(lt.UUID)
		}
	}

	if ct := s.ConvertedType; ct != nil {
		// This column contains no logical type but has a converted type, it
		// was likely created by an older parquet writer. Convert the legacy
		// type representation to the equivalent logical parquet type.
		switch *ct {
		case deprecated.UTF8:
			return &stringType{}
		case deprecated.Map:
			return &mapType{}
		case deprecated.MapKeyValue:
			return &groupType{}
		case deprecated.List:
			return &listType{}
		case deprecated.Enum:
			return &enumType{}
		case deprecated.Decimal:
			// TODO
		case deprecated.Date:
			return &dateType{}
		case deprecated.TimeMillis:
			return &timeType{IsAdjustedToUTC: true, Unit: Millisecond.TimeUnit()}
		case deprecated.TimeMicros:
			return &timeType{IsAdjustedToUTC: true, Unit: Microsecond.TimeUnit()}
		case deprecated.TimestampMillis:
			return &timestampType{IsAdjustedToUTC: true, Unit: Millisecond.TimeUnit()}
		case deprecated.TimestampMicros:
			return &timestampType{IsAdjustedToUTC: true, Unit: Microsecond.TimeUnit()}
		case deprecated.Uint8:
			return &unsignedIntTypes[0]
		case deprecated.Uint16:
			return &unsignedIntTypes[1]
		case deprecated.Uint32:
			return &unsignedIntTypes[2]
		case deprecated.Uint64:
			return &unsignedIntTypes[3]
		case deprecated.Int8:
			return &signedIntTypes[0]
		case deprecated.Int16:
			return &signedIntTypes[1]
		case deprecated.Int32:
			return &signedIntTypes[2]
		case deprecated.Int64:
			return &signedIntTypes[3]
		case deprecated.Json:
			return &jsonType{}
		case deprecated.Bson:
			return &bsonType{}
		case deprecated.Interval:
			// TODO
		}
	}

	if t := s.Type; t != nil {
		// The column only has a physical type, convert it to one of the
		// primitive types supported by this package.
		switch kind := Kind(*t); kind {
		case Boolean:
			return BooleanType
		case Int32:
			return Int32Type
		case Int64:
			return Int64Type
		case Int96:
			return Int96Type
		case Float:
			return FloatType
		case Double:
			return DoubleType
		case ByteArray:
			return ByteArrayType
		case FixedLenByteArray:
			if s.TypeLength != nil {
				return FixedLenByteArrayType(int(*s.TypeLength))
			}
		}
	}

	// If we reach this point, we are likely reading a parquet column that was
	// written with a non-standard type or is in a newer version of the format
	// than this package supports.
	return &nullType{}
}

func schemaRepetitionTypeOf(s *format.SchemaElement) format.FieldRepetitionType {
	if s.RepetitionType != nil {
		return *s.RepetitionType
	}
	return format.Required
}

var (
	_ Node        = (*Column)(nil)
	_ IndexedNode = (*Column)(nil)
)
