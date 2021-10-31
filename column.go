package parquet

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/segmentio/parquet/schema"
)

// Column represents a column in a parquet file.
type Column struct {
	file    *File
	schema  *schema.SchemaElement
	order   *schema.ColumnOrder
	columns []*Column
}

// Type returns the Go value type of the column. If c is not a leaf column, the
// method returns nil.
//
// The returned type will be one of those:
//
//	BOOLEAN              → bool
//	INT32                → int32
//	INT64                → int64
//	INT96                → [12]byte
//	FLOAT                → float32
//	DOUBLE               → float64
//	BYTE_ARRAY           → []byte
//	FIXED_LEN_BYTE_ARRAY → [?]byte
//
func (c *Column) Type() reflect.Type {
	if c.columns != nil {
		return nil
	}
	switch c.schema.Type {
	case schema.Boolean:
		return reflect.TypeOf(false)
	case schema.Int32:
		return reflect.TypeOf(int32(0))
	case schema.Int64:
		return reflect.TypeOf(int64(0))
	case schema.Int96:
		return reflect.ArrayOf(12, reflect.TypeOf(byte(0)))
	case schema.Float:
		return reflect.TypeOf(float32(0))
	case schema.Double:
		return reflect.TypeOf(float64(0))
	case schema.ByteArray:
		return reflect.TypeOf(([]byte)(nil))
	case schema.FixedLenByteArray:
		return reflect.ArrayOf(int(c.schema.TypeLength), reflect.TypeOf(byte(0)))
	default:
		panic(fmt.Errorf("cannot convert parquet type %#x to a go valuue type", c.schema.Type))
	}
}

// Required returns true if the column is required.
func (c *Column) Required() bool {
	return c.schema.RepetitionType == schema.Required
}

// Optional returns true if the column is optional.
func (c *Column) Optional() bool {
	return c.schema.RepetitionType == schema.Optional
}

// Repeated returns true if the column may repeat.
func (c *Column) Repeated() bool {
	return c.schema.RepetitionType == schema.Repeated
}

// String returns a human-redable string representation of the oclumn.
func (c *Column) String() string {
	switch {
	case c.columns != nil:
		return fmt.Sprintf("%s{%s}",
			c.schema.Name,
			c.schema.RepetitionType)

	case c.schema.Type == schema.FixedLenByteArray:
		return fmt.Sprintf("%s{%s(%d),%s}",
			c.schema.Name,
			c.schema.Type,
			c.schema.TypeLength,
			c.schema.RepetitionType)

	default:
		return fmt.Sprintf("%s{%s,%s}",
			c.schema.Name,
			c.schema.Type,
			c.schema.RepetitionType)
	}
}

// Name returns the column name.
func (c *Column) Name() string {
	return c.schema.Name
}

// Columns returns the list of child columns.
//
// The method returns the same slice across multiple calls, the program must
// treat it as a read-only value.
func (c *Column) Columns() []*Column {
	return c.columns
}

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

func openColumns(file *File, schemaIndex, columnOrderIndex int) (*Column, int, int, error) {
	c := &Column{
		file:   file,
		schema: &file.metadata.Schema[schemaIndex],
	}

	schemaIndex++
	numChildren := int(c.schema.NumChildren)

	if numChildren == 0 {
		if columnOrderIndex < len(file.metadata.ColumnOrders) {
			c.order = &file.metadata.ColumnOrders[columnOrderIndex]
			columnOrderIndex++
		}
		return c, schemaIndex, columnOrderIndex, nil
	}

	c.columns = make([]*Column, numChildren)

	for i := range c.columns {
		if schemaIndex >= len(file.metadata.Schema) {
			return nil, schemaIndex, columnOrderIndex,
				fmt.Errorf("column %q has more children than there are schemas in the file: %d > %d", c.schema.Name, schemaIndex+1, len(file.metadata.Schema))
		}

		var err error
		c.columns[i], schemaIndex, columnOrderIndex, err = openColumns(file, schemaIndex, columnOrderIndex)
		if err != nil {
			return nil, schemaIndex, columnOrderIndex, fmt.Errorf("%s: %w", c.schema.Name, err)
		}
	}

	return c, schemaIndex, columnOrderIndex, nil
}
