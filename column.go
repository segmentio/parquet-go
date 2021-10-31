package parquet

import (
	"fmt"
	"sort"

	"github.com/segmentio/parquet/schema"
)

type Column struct {
	file    *File
	schema  *schema.SchemaElement
	order   *schema.ColumnOrder
	columns []*Column
}

func (c *Column) Name() string {
	return c.schema.Name
}

func (c *Column) Columns() []*Column {
	return c.columns
}

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
		schema: &file.Schema[schemaIndex],
	}

	schemaIndex++

	if c.schema.Type != 0 {
		if columnOrderIndex < len(file.ColumnOrders) {
			c.order = &file.ColumnOrders[columnOrderIndex]
			columnOrderIndex++
		}
		return c, schemaIndex, columnOrderIndex, nil
	}

	numChildren := int(c.schema.NumChildren)
	c.columns = make([]*Column, numChildren)

	for i := range c.columns {
		if schemaIndex >= len(file.Schema) {
			return nil, schemaIndex, columnOrderIndex,
				fmt.Errorf("column %q has more children than there are schemas in the file: %d > %d", c.schema.Name, schemaIndex+1, len(file.Schema))
		}

		var err error
		c.columns[i], schemaIndex, columnOrderIndex, err = openColumns(file, schemaIndex, columnOrderIndex)
		if err != nil {
			return nil, schemaIndex, columnOrderIndex, fmt.Errorf("%s: %w", c.schema.Name, err)
		}
	}

	return c, schemaIndex, columnOrderIndex, nil
}
