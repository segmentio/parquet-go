package parquet

import (
	"fmt"
	"sort"

	"github.com/segmentio/parquet/schema"
)

type Column struct {
	file    *File
	schema  *schema.SchemaElement
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

func openColumns(file *File, index int) (*Column, int, error) {
	c := &Column{
		file:   file,
		schema: &file.Schema[index],
	}

	if c.schema.Type != 0 {
		return c, index + 1, nil
	}

	index++
	numChildren := int(c.schema.NumChildren)
	c.columns = make([]*Column, numChildren)

	for i := range c.columns {
		if index >= len(file.Schema) {
			return nil, index, fmt.Errorf("column %q has more children than there are columns in the file: %d > %d", c.schema.Name, index+1, len(file.Schema))
		}
		child, nextIndex, err := openColumns(file, index)
		if err != nil {
			return nil, nextIndex, fmt.Errorf("%s: %w", c.schema.Name, err)
		}
		index = nextIndex
		c.columns[i] = child
	}

	return c, index, nil
}
