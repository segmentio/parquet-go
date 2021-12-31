package parquet

import (
	"sort"

	"github.com/segmentio/parquet/format"
)

// Buffer represents an in-memory group of parquet rows.
//
// The main purpose of the Buffer type is to provide a way to sort rows before
// writting them to a parquet file. Buffer implements sort.Interface as a way
// to support reordering the rows that have been written to it.
type Buffer struct {
	config  *BufferConfig
	schema  *Schema
	rowbuf  []Value
	colbuf  [][]Value
	columns []BufferColumn
	sorted  []BufferColumn
	sorting []format.SortingColumn
	numRows int
}

func NewBuffer(options ...BufferOption) *Buffer {
	config := DefaultBufferConfig()
	config.Apply(options...)
	if err := config.Validate(); err != nil {
		panic(err)
	}
	rg := &Buffer{
		config: config,
	}
	if config.Schema != nil {
		rg.configure(config.Schema)
	}
	return rg
}

func (rg *Buffer) configure(schema *Schema) {
	rg.schema = schema
	rg.init(schema, make([]string, 0, 10), 0, 0, rg.config)
	rg.rowbuf = make([]Value, 0, 10)
	rg.colbuf = make([][]Value, len(rg.columns))
	rg.sorted = make([]BufferColumn, len(rg.sorting))
	for i, sort := range rg.sorting {
		rg.sorted[i] = rg.columns[sort.ColumnIdx]
	}
}

func (rg *Buffer) init(node Node, path []string, repetitionLevel, definitionLevel int8, config *BufferConfig) {
	switch {
	case node.Optional():
		definitionLevel++
	case node.Repeated():
		repetitionLevel++
		definitionLevel++
	}

	if isLeaf(node) {
		nullOrdering := nullsGoLast
		columnIndex := len(rg.columns)
		columnType := node.Type()
		bufferSize := config.ColumnBufferSize
		dictionary := (Dictionary)(nil)
		encoding, _ := encodingAndCompressionOf(node)

		if isDictionaryEncoding(encoding) {
			bufferSize /= 2
			dictionary = columnType.NewDictionary(bufferSize)
			columnType = dictionary.Type()
		}

		column := columnType.NewBufferColumn(bufferSize)

		for _, sorting := range config.SortingColumns {
			if stringsAreEqual(sorting.Path(), path) {
				sortingColumn := format.SortingColumn{
					ColumnIdx:  int32(columnIndex),
					Descending: sorting.Descending(),
					NullsFirst: sorting.NullsFirst(),
				}
				if sortingColumn.Descending {
					column = &reversedBufferColumn{column}
				}
				if sortingColumn.NullsFirst {
					nullOrdering = nullsGoFirst
				}
				rg.sorting = append(rg.sorting, sortingColumn)
				break
			}
		}

		switch {
		case repetitionLevel > 0:
			column = newRepeatedBufferColumn(column, repetitionLevel, definitionLevel, nullOrdering)
		case definitionLevel > 0:
			column = newOptionalBufferColumn(column, definitionLevel, nullOrdering)
		}

		rg.columns = append(rg.columns, column)
		return
	}

	i := len(path)
	path = append(path, "")

	for _, name := range node.ChildNames() {
		path[i] = name
		rg.init(node.ChildByName(name), path, repetitionLevel, definitionLevel, config)
	}
}

func (rg *Buffer) Size() int64 {
	size := int64(0)
	for _, col := range rg.columns {
		size += col.Size()
	}
	return size
}

func (rg *Buffer) Columns() []RowGroupColumn {
	columns := make([]RowGroupColumn, len(rg.columns))
	for i, c := range rg.columns {
		columns[i] = c
	}
	return columns
}

func (rg *Buffer) NumRows() int { return rg.numRows }

func (rg *Buffer) Schema() *Schema { return rg.schema }

func (rg *Buffer) SortingColumns() []format.SortingColumn { return rg.sorting }

func (rg *Buffer) Len() int { return rg.numRows }

func (rg *Buffer) Less(i, j int) bool {
	for _, col := range rg.sorted {
		switch {
		case col.Less(i, j):
			return true
		case col.Less(j, i): // not equal?
			return false
		}
	}
	return false
}

func (rg *Buffer) Swap(i, j int) {
	for _, col := range rg.columns {
		col.Swap(i, j)
	}
}

func (rg *Buffer) Reset() {
	for _, col := range rg.columns {
		col.Reset()
	}
	rg.numRows = 0
}

func (rg *Buffer) Write(row interface{}) error {
	if rg.schema == nil {
		rg.configure(SchemaOf(row))
	}
	defer func() {
		clearValues(rg.rowbuf)
	}()
	rg.rowbuf = rg.schema.Deconstruct(rg.rowbuf[:0], row)
	return rg.WriteRow(rg.rowbuf)
}

func (rg *Buffer) WriteRow(row Row) error {
	defer func() {
		for i, colbuf := range rg.colbuf {
			clearValues(colbuf)
			rg.colbuf[i] = colbuf[:0]
		}
	}()

	for _, value := range row {
		columnIndex := value.ColumnIndex()
		rg.colbuf[columnIndex] = append(rg.colbuf[columnIndex], value)
	}

	for columnIndex, values := range rg.colbuf {
		if err := rg.columns[columnIndex].WriteRow(values); err != nil {
			return err
		}
	}

	rg.numRows++
	return nil
}

/*
type bufferRowReader struct {
	columns []bufferColumnRowReader
}

func (r *bufferColumnRowReader) ReadRow(row Row) (Row, error) {
	var reset = len(row)
	var err error

	for i := range r.columns {
		c := &r.columns[i]
		n := len(row)

		if row, err = c.ReadRow(row); err != nil {
			return row[:reset], err
		}
		for columnIndex := ^int8(i); n < len(row); n++ {
			row[n].columnIndex = columnIndex
		}
	}

	return row, nil
}

type bufferColumnRowReader struct {
	column BufferColumn
	offset int
}

func (r *bufferColumnRowReader) ReadRow(row Row) (Row, error) {
	row, err := r.column.ReadRowAt(row, r.offset)
	r.offset++
	return row, err
}
*/

var (
	_ sort.Interface = (*Buffer)(nil)
)
