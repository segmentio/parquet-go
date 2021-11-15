package parquet

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/format"
)

// Column represents a column in a parquet file.
//
// Methods of Column values are safe to call concurrently from multiple
// goroutines.
//
// Column instances satisfy the Node interface.
type Column struct {
	file    *File
	schema  *format.SchemaElement
	order   *format.ColumnOrder
	names   []string
	columns []*Column
	chunks  []*format.ColumnChunk

	depth              int32
	maxRepetitionLevel int32
	maxDefinitionLevel int32
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
func (c *Column) Required() bool { return c.schema.RepetitionType == format.Required }

// Optional returns true if the column is optional.
func (c *Column) Optional() bool { return c.schema.RepetitionType == format.Optional }

// Repeated returns true if the column may repeat.
func (c *Column) Repeated() bool { return c.schema.RepetitionType == format.Repeated }

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

// Object constructs a parquet object from the Go value passed as argument.
//
// This method contributes to satisfying the Node interface.
func (c *Column) Object(value reflect.Value) Object {
	panic("NOT IMPLEMENTED (TODO)")
}

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
			c.schema.Name,
			c.schema.RepetitionType,
			c.maxRepetitionLevel,
			c.maxDefinitionLevel)

	case c.Type().Kind() == FixedLenByteArray:
		return fmt.Sprintf("%s{%s(%d),%s,R=%d,D=%d}",
			c.schema.Name,
			c.schema.Type,
			c.schema.TypeLength,
			c.schema.RepetitionType,
			c.maxRepetitionLevel,
			c.maxDefinitionLevel)

	default:
		return fmt.Sprintf("%s{%s,%s,R=%d,D=%d}",
			c.schema.Name,
			c.schema.Type,
			c.schema.RepetitionType,
			c.maxRepetitionLevel,
			c.maxDefinitionLevel)
	}
}

func openColumns(file *File) (*Column, error) {
	cl := columnLoader{}

	c, err := cl.open(file)
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

	setMaxLevels(c, 0, 0, 0)
	return c, nil
}

func setMaxLevels(col *Column, depth, repetition, definition int32) {
	switch col.schema.RepetitionType {
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
	for _, c := range col.columns {
		setMaxLevels(c, depth, repetition, definition)
	}
}

type columnLoader struct {
	schemaIndex         int
	columnOrderIndex    int
	rowGroupColumnIndex int
}

func (cl *columnLoader) open(file *File) (*Column, error) {
	c := &Column{
		file:   file,
		schema: &file.metadata.Schema[cl.schemaIndex],
	}

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
		c.columns[i], err = cl.open(file)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.schema.Name, err)
		}
	}

	for i, col := range c.columns {
		c.names[i] = col.Name()
	}

	sort.Sort(columnsByName{c})
	return c, nil
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
	return int(t.TypeLength)
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

var (
	_ Node = (*Column)(nil)
)
