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

func (c *concatenatedColumnChunk) NumValues() int64 {
	n := int64(0)
	for i := range c.chunks {
		n += c.chunks[i].NumValues()
	}
	return n
}

func (c *concatenatedColumnChunk) Column() int {
	return c.column
}

func (c *concatenatedColumnChunk) Pages() Pages {
	return &concatenatedPages{column: c}
}

func (c *concatenatedColumnChunk) ColumnIndex() ColumnIndex {
	// TODO: implement
	return nil
}

func (c *concatenatedColumnChunk) OffsetIndex() OffsetIndex {
	// TODO: implement
	return nil
}

func (c *concatenatedColumnChunk) BloomFilter() BloomFilter {
	return concatenatedBloomFilter{c}
}

type concatenatedBloomFilter struct{ *concatenatedColumnChunk }

func (f concatenatedBloomFilter) ReadAt(b []byte, off int64) (int, error) {
	i := 0

	for i < len(f.chunks) {
		if r := f.chunks[i].BloomFilter(); r != nil {
			size := r.Size()
			if off < size {
				break
			}
			off -= size
		}
		i++
	}

	if i == len(f.chunks) {
		return 0, io.EOF
	}

	rn := int(0)
	for len(b) > 0 {
		if r := f.chunks[i].BloomFilter(); r != nil {
			n, err := r.ReadAt(b, off)
			rn += n
			if err != nil {
				return rn, err
			}
			if b = b[n:]; len(b) == 0 {
				return rn, nil
			}
			off += int64(n)
		}
		i++
	}

	if i == len(f.chunks) {
		return rn, io.EOF
	}
	return rn, nil
}

func (f concatenatedBloomFilter) Size() int64 {
	size := int64(0)
	for _, c := range f.chunks {
		if b := c.BloomFilter(); b != nil {
			size += b.Size()
		}
	}
	return size
}

func (f concatenatedBloomFilter) Check(key []byte) (bool, error) {
	for _, c := range f.chunks {
		if b := c.BloomFilter(); b != nil {
			if ok, err := b.Check(key); ok || err != nil {
				return ok, err
			}
		}
	}
	return false, nil
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
