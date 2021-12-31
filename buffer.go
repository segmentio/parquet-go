package parquet

import (
	"io"
	"sort"
)

// Buffer represents an in-memory group of parquet rows.
//
// The main purpose of the Buffer type is to provide a way to sort rows before
// writting them to a parquet file. Buffer implements sort.Interface as a way
// to support reordering the rows that have been written to it.
type Buffer struct {
	config  *RowGroupConfig
	schema  *Schema
	rowbuf  []Value
	colbuf  [][]Value
	columns []BufferColumn
	sorted  []BufferColumn
	readRow columnReadRowFunc
	numRows int
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

		column := columnType.NewBufferColumn(bufferSize)
		switch {
		case leaf.maxRepetitionLevel > 0:
			column = newRepeatedBufferColumn(column, leaf.maxRepetitionLevel, leaf.maxDefinitionLevel, nullOrdering)
		case leaf.maxDefinitionLevel > 0:
			column = newOptionalBufferColumn(column, leaf.maxDefinitionLevel, nullOrdering)
		}
		buf.columns = append(buf.columns, column)

		if sorting := sortingColumnOf(buf.config.SortingColumns, leaf.path); sorting != nil {
			if sorting.Descending() {
				column = &reversedBufferColumn{column}
			}
			if sorting.NullsFirst() {
				nullOrdering = nullsGoFirst
			}
			buf.sorted = append(buf.sorted, column)
		}
	})

	buf.schema = schema
	buf.rowbuf = make([]Value, 0, 10)
	buf.colbuf = make([][]Value, len(buf.columns))
	_, buf.readRow = columnReadRowFuncOf(schema, 0, 0)
}

// Size returns the estimated size of the buffer in memory.
func (buf *Buffer) Size() int64 {
	size := int64(0)
	for _, col := range buf.columns {
		size += col.Size()
	}
	return size
}

// Columns returns the list of columns in the buffer.
//
// The list will be empty until a schema is configured on the buffer.
func (buf *Buffer) Columns() []RowGroupColumn {
	columns := make([]RowGroupColumn, len(buf.columns))
	for i, c := range buf.columns {
		columns[i] = c
	}
	return columns
}

// NumRows returns the number of rows written to the buffer.
func (buf *Buffer) NumRows() int { return buf.numRows }

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
func (buf *Buffer) Len() int { return buf.numRows }

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
	buf.numRows = 0
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

	for _, value := range row {
		columnIndex := value.ColumnIndex()
		buf.colbuf[columnIndex] = append(buf.colbuf[columnIndex], value)
	}

	for columnIndex, values := range buf.colbuf {
		if err := buf.columns[columnIndex].WriteRow(values); err != nil {
			return err
		}
	}

	buf.numRows++
	return nil
}

// Rows returns a reader exposing the current content of the buffer.
//
// The buffer and the returned reader share memory, mutating the buffer
// concurrently to reading rows may result in non-deterministic behavior.
func (buf *Buffer) Rows() RowReader { return &bufferRowReader{buffer: buf} }

type bufferRowReader struct {
	buffer  *Buffer
	readRow columnReadRowFunc
	readers []columnValueReader
}

func (r *bufferRowReader) initReadRow() {
	r.readers = makeColumnValueReaders(len(r.buffer.columns), func(i int) ValueReader {
		return r.buffer.columns[i].Values()
	})
}

func (r *bufferRowReader) ReadRow(row Row) (Row, error) {
	if len(r.readers) == 0 {
		r.initReadRow()
	}
	if r.readRow == nil {
		return row, io.EOF
	}
	n := len(row)
	row, err := r.readRow(row, 0, r.readers)
	if err == nil && len(row) == n {
		err = io.EOF
	}
	return row, err
}

func (r *bufferRowReader) WriteRowsTo(w RowWriter) (int64, error) {
	if rgw, ok := w.(RowGroupWriter); ok {
		return rgw.WriteRowGroup(r.buffer)
	}
	if rt, ok := w.(RowReaderFrom); ok {
		return rt.ReadRowsFrom(r)
	}
	return CopyRows(w, struct{ RowReader }{r})
}

func (r *bufferRowReader) Schema() *Schema { return r.buffer.schema }

var (
	_ RowReaderWithSchema = (*bufferRowReader)(nil)
	_ RowWriterTo         = (*bufferRowReader)(nil)
	_ RowGroup            = (*Buffer)(nil)
	_ sort.Interface      = (*Buffer)(nil)
)
