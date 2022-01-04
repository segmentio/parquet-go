package parquet

import (
	"sort"
)

// Buffer represents an in-memory group of parquet rows.
//
// The main purpose of the Buffer type is to provide a way to sort rows before
// writing them to a parquet file. Buffer implements sort.Interface as a way
// to support reordering the rows that have been written to it.
type Buffer struct {
	config  *RowGroupConfig
	schema  *Schema
	rowbuf  []Value
	colbuf  [][]Value
	columns []ColumnBuffer
	sorted  []ColumnBuffer
}

// NewBuffer constructs a new buffer, using the given list of buffer options
// to configure the buffer returned by the function.
func NewBuffer(options ...RowGroupOption) *Buffer {
	config := DefaultRowGroupConfig()
	config.Apply(options...)
	if err := config.Validate(); err != nil {
		panic(err)
	}
	buf := &Buffer{
		config: config,
	}
	if config.Schema != nil {
		buf.configure(config.Schema)
	}
	return buf
}

func (buf *Buffer) configure(schema *Schema) {
	if schema == nil {
		return
	}
	sortingColumns := buf.config.SortingColumns
	buf.sorted = make([]ColumnBuffer, len(sortingColumns))

	forEachLeafColumnOf(schema, func(leaf leafColumn) {
		nullOrdering := nullsGoLast
		columnType := leaf.node.Type()
		bufferSize := buf.config.ColumnBufferSize
		dictionary := (Dictionary)(nil)
		encoding, _ := encodingAndCompressionOf(leaf.node)

		if isDictionaryEncoding(encoding) {
			bufferSize /= 2
			dictionary = columnType.NewDictionary(bufferSize)
			columnType = dictionary.Type()
		}

		column := columnType.NewColumnBuffer(leaf.columnIndex, bufferSize)
		switch {
		case leaf.maxRepetitionLevel > 0:
			column = newRepeatedColumnBuffer(column, leaf.maxRepetitionLevel, leaf.maxDefinitionLevel, nullOrdering)
		case leaf.maxDefinitionLevel > 0:
			column = newOptionalColumnBuffer(column, leaf.maxDefinitionLevel, nullOrdering)
		}
		buf.columns = append(buf.columns, column)

		if sortingIndex := searchSortingColumn(sortingColumns, leaf.path); sortingIndex < len(sortingColumns) {
			if sortingColumns[sortingIndex].Descending() {
				column = &reversedColumnBuffer{column}
			}
			if sortingColumns[sortingIndex].NullsFirst() {
				nullOrdering = nullsGoFirst
			}
			buf.sorted[sortingIndex] = column
		}
	})

	buf.schema = schema
	buf.rowbuf = make([]Value, 0, 10)
	buf.colbuf = make([][]Value, len(buf.columns))
}

// Size returns the estimated size of the buffer in memory.
func (buf *Buffer) Size() int64 {
	size := int64(0)
	for _, col := range buf.columns {
		size += col.Size()
	}
	return size
}

// NumRows returns the number of rows written to the buffer.
func (buf *Buffer) NumRows() int {
	if len(buf.columns) == 0 {
		return 0
	} else {
		// All columns have the same number of rows.
		return buf.columns[0].Len()
	}
}

// NumColumns returns the number of columns in the buffer.
//
// The count will be zero until a schema is configured on buf.
func (buf *Buffer) NumColumns() int { return len(buf.columns) }

// Column returns the buffer column at index i.
//
// The method panics if i is negative or beyond the last column index in buf.
func (buf *Buffer) Column(i int) RowGroupColumn { return buf.columns[i] }

// Schema returns the schema of the buffer.
//
// The schema is either configured by passing a Schema in the option list when
// constructing the buffer, or lazily discovered when the first row is written.
func (buf *Buffer) Schema() *Schema { return buf.schema }

// SortingColumns returns the list of columns by which the buffer will be
// sorted.
//
// The sorting order is configured by passing a SortingColumns option when
// constructing the buffer.
func (buf *Buffer) SortingColumns() []SortingColumn { return buf.config.SortingColumns }

// Len returns the number of rows written to the buffer.
func (buf *Buffer) Len() int { return buf.NumRows() }

// Less returns true if the row[i] < row[j] in the buffer.
func (buf *Buffer) Less(i, j int) bool {
	for _, col := range buf.sorted {
		switch {
		case col.Less(i, j):
			return true
		case col.Less(j, i):
			return false
		}
	}
	return false
}

// Swap exchanges the rows at indexes i and j.
func (buf *Buffer) Swap(i, j int) {
	for _, col := range buf.columns {
		col.Swap(i, j)
	}
}

// Reset clears the content of the buffer, allowing it to be reused.
func (buf *Buffer) Reset() {
	for _, col := range buf.columns {
		col.Reset()
	}
}

// Write writes a row held in a Go value to the buffer.
func (buf *Buffer) Write(row interface{}) error {
	if buf.schema == nil {
		buf.configure(SchemaOf(row))
	}
	defer func() {
		clearValues(buf.rowbuf)
	}()
	buf.rowbuf = buf.schema.Deconstruct(buf.rowbuf[:0], row)
	return buf.WriteRow(buf.rowbuf)
}

// WriteRow writes a parquet row to the buffer.
func (buf *Buffer) WriteRow(row Row) error {
	defer func() {
		for i, colbuf := range buf.colbuf {
			clearValues(colbuf)
			buf.colbuf[i] = colbuf[:0]
		}
	}()

	if buf.schema == nil {
		return ErrRowGroupSchemaMissing
	}

	for _, value := range row {
		columnIndex := value.Column()
		buf.colbuf[columnIndex] = append(buf.colbuf[columnIndex], value)
	}

	for columnIndex, values := range buf.colbuf {
		if err := buf.columns[columnIndex].WriteRow(values); err != nil {
			return err
		}
	}

	return nil
}

// WriteRowGroup satisfies the RowGroupWriter interface.
func (buf *Buffer) WriteRowGroup(rowGroup RowGroup) (int64, error) {
	rowGroupSchema := rowGroup.Schema()
	switch {
	case rowGroupSchema == nil:
		return 0, ErrRowGroupSchemaMissing
	case buf.schema == nil:
		buf.configure(rowGroupSchema)
	case !nodesAreEqual(buf.schema, rowGroupSchema):
		return 0, ErrRowGroupSchemaMismatch
	}
	if !sortingColumnsHavePrefix(rowGroup.SortingColumns(), buf.SortingColumns()) {
		return 0, ErrRowGroupSortingColumnsMismatch
	}
	n := buf.NumRows()
	_, err := CopyPages(buf, rowGroup.Pages())
	return int64(buf.NumRows() - n), err
}

// WritePage satisfies the PageWriter interface.
func (buf *Buffer) WritePage(page Page) (int64, error) {
	return CopyValues(buf.columns[page.Column()], page.Values())
}

// Rows returns a reader exposing the current content of the buffer.
//
// The buffer and the returned reader share memory, mutating the buffer
// concurrently to reading rows may result in non-deterministic behavior.
func (buf *Buffer) Rows() RowReader { return &rowGroupRowReader{rowGroup: buf} }

// Pages returns a reader exposing the current pages of the buffer.
//
// The buffer and the returned reader share memory, mutating the buffer
// concurrently to reading rows may result in non-deterministic behavior.
func (buf *Buffer) Pages() PageReader { return &rowGroupPageReader{rowGroup: buf} }

var (
	_ RowGroup       = (*Buffer)(nil)
	_ RowGroupWriter = (*Buffer)(nil)
	_ PageWriter     = (*Buffer)(nil)
	_ sort.Interface = (*Buffer)(nil)
)
