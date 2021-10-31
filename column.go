package parquet

import (
	"fmt"
	"sort"

	"github.com/segmentio/parquet/schema"
)

// Column represents a column in a parquet file.
//
// Methods of Column values are safe to call concurrently from multiple
// goroutines.
type Column struct {
	file    *File
	schema  *schema.SchemaElement
	order   *schema.ColumnOrder
	columns []*Column
	chunks  []*schema.ColumnChunk
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

// Scan returns a row iterator which can be used to scan through rows of
// of the column.
func (c *Column) Scan() *ColumnRows {
	return &ColumnRows{column: c}
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

	return c, nil
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

		c.chunks = make([]*schema.ColumnChunk, 0, len(file.metadata.RowGroups))
		for index, rowGroup := range file.metadata.RowGroups {
			if cl.rowGroupColumnIndex >= len(rowGroup.Columns) {
				return nil, fmt.Errorf("row group at index %d does not have enough columns", index)
			}
			c.chunks = append(c.chunks, &rowGroup.Columns[cl.rowGroupColumnIndex])
		}
		cl.rowGroupColumnIndex++

		return c, nil
	}

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

	return c, nil
}
