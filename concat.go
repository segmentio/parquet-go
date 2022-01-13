package parquet

import (
	"io"
)

func concat(schema *Schema, rowGroups []RowGroup) *concatenatedRowGroup {
	c := new(concatenatedRowGroup)
	c.init(schema, rowGroups)
	return c
}

func (c *concatenatedRowGroup) init(schema *Schema, rowGroups []RowGroup) {
	c.schema = schema
	c.rowGroups = rowGroups
	c.columns = make([]concatenatedColumnChunk, numColumnsOf(schema))

	for i := range c.columns {
		c.columns[i].rowGroup = c
		c.columns[i].column = i
		c.columns[i].chunks = make([]ColumnChunk, len(rowGroups))

		for j, rowGroup := range rowGroups {
			c.columns[i].chunks[j] = rowGroup.Column(i)
		}
	}
}

type concatenatedRowGroup struct {
	schema    *Schema
	rowGroups []RowGroup
	columns   []concatenatedColumnChunk
}

func (c *concatenatedRowGroup) NumRows() (numRows int64) {
	for _, rowGroup := range c.rowGroups {
		numRows += rowGroup.NumRows()
	}
	return numRows
}

func (c *concatenatedRowGroup) NumColumns() int { return len(c.columns) }

func (c *concatenatedRowGroup) Column(i int) ColumnChunk { return &c.columns[i] }

func (c *concatenatedRowGroup) SortingColumns() []SortingColumn { return nil }

func (c *concatenatedRowGroup) Schema() *Schema { return c.schema }

func (c *concatenatedRowGroup) Rows() Rows { return &rowGroupRowReader{rowGroup: c} }

type concatenatedColumnChunk struct {
	rowGroup *concatenatedRowGroup
	column   int
	chunks   []ColumnChunk
}

func (c *concatenatedColumnChunk) Type() Type {
	if len(c.chunks) != 0 {
		return c.chunks[0].Type() // all chunks should be of the same type
	}
	return nil
}

func (c *concatenatedColumnChunk) Column() int {
	return c.column
}

func (c *concatenatedColumnChunk) Pages() Pages {
	return &concatenatedPages{column: c}
}

func (c *concatenatedColumnChunk) ColumnIndex() ColumnIndex {
	// TODO: changin the ColumnIndex type from a concrete type to an interface
	// means that we could create a concatenated view of the indexes instead of
	// having to reallocate memory buffers.
	return nil
}

func (c *concatenatedColumnChunk) OffsetIndex() OffsetIndex {
	// TODO: we cannot really reconstruct the offsets here because we do not
	// know whether the parent row groups belong to the same file.
	//
	// Here as well, changing the OffsetIndex type to an interface could let us
	// embed useful information to map the index back to the original chunk and
	// allow leveraging it to lookup pages, even if there are no absolute file
	// offset.
	return nil
}

type concatenatedPages struct {
	pages  Pages
	index  int
	column *concatenatedColumnChunk
}

func (r *concatenatedPages) ReadPage() (Page, error) {
	for {
		if r.pages != nil {
			p, err := r.pages.ReadPage()
			if err == nil || err != io.EOF {
				return p, err
			}
			r.pages = nil
		}
		if r.index == len(r.column.chunks) {
			return nil, io.EOF
		}
		r.pages = r.column.chunks[r.index].Pages()
		r.index++
	}
}

func (r *concatenatedPages) SeekToRow(rowIndex int64) error {
	rowGroups := r.column.rowGroup.rowGroups
	numRows := int64(0)
	r.pages = nil
	r.index = 0

	for r.index < len(rowGroups) {
		numRows = rowGroups[r.index].NumRows()
		if rowIndex < numRows {
			break
		}
		rowIndex -= numRows
		r.index++
	}

	if r.index < len(rowGroups) {
		r.pages = r.column.chunks[r.index].Pages()
		r.index++
		return r.pages.SeekToRow(rowIndex)
	}
	return nil
}
