package parquet

import (
	"io"
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
	readRow columnReadRowFunc
	numRows int
}

func NewBuffer(options ...BufferOption) *Buffer {
	config := DefaultBufferConfig()
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
	buf.schema = schema
	buf.init(schema, make([]string, 0, 10), 0, 0, buf.config)
	buf.rowbuf = make([]Value, 0, 10)
	buf.colbuf = make([][]Value, len(buf.columns))
	buf.sorted = make([]BufferColumn, len(buf.sorting))
	for i, sort := range buf.sorting {
		buf.sorted[i] = buf.columns[sort.ColumnIdx]
	}
	_, buf.readRow = columnReadRowFuncOf(schema, 0, 0)
}

func (buf *Buffer) init(node Node, path []string, repetitionLevel, definitionLevel int8, config *BufferConfig) {
	switch {
	case node.Optional():
		definitionLevel++
	case node.Repeated():
		repetitionLevel++
		definitionLevel++
	}

	if isLeaf(node) {
		nullOrdering := nullsGoLast
		columnIndex := len(buf.columns)
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
				buf.sorting = append(buf.sorting, sortingColumn)
				break
			}
		}

		switch {
		case repetitionLevel > 0:
			column = newRepeatedBufferColumn(column, repetitionLevel, definitionLevel, nullOrdering)
		case definitionLevel > 0:
			column = newOptionalBufferColumn(column, definitionLevel, nullOrdering)
		}

		buf.columns = append(buf.columns, column)
		return
	}

	i := len(path)
	path = append(path, "")

	for _, name := range node.ChildNames() {
		path[i] = name
		buf.init(node.ChildByName(name), path, repetitionLevel, definitionLevel, config)
	}
}

func (buf *Buffer) Size() int64 {
	size := int64(0)
	for _, col := range buf.columns {
		size += col.Size()
	}
	return size
}

func (buf *Buffer) Columns() []RowGroupColumn {
	columns := make([]RowGroupColumn, len(buf.columns))
	for i, c := range buf.columns {
		columns[i] = c
	}
	return columns
}

func (buf *Buffer) NumRows() int { return buf.numRows }

func (buf *Buffer) Schema() *Schema { return buf.schema }

func (buf *Buffer) SortingColumns() []format.SortingColumn { return buf.sorting }

func (buf *Buffer) Len() int { return buf.numRows }

func (buf *Buffer) Less(i, j int) bool {
	for _, col := range buf.sorted {
		switch {
		case col.Less(i, j):
			return true
		case col.Less(j, i): // not equal?
			return false
		}
	}
	return false
}

func (buf *Buffer) Swap(i, j int) {
	for _, col := range buf.columns {
		col.Swap(i, j)
	}
}

func (buf *Buffer) Reset() {
	for _, col := range buf.columns {
		col.Reset()
	}
	buf.numRows = 0
}

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

func (buf *Buffer) Rows() RowReader {
	return &bufferRowReader{buffer: buf}
}

type bufferRowReader struct {
	buffer  *Buffer
	readRow columnReadRowFunc
	readers []columnValueReader
}

func (r *bufferRowReader) initReadRow() {
	const columnBufferSize = 170
	buffer := make([]Value, columnBufferSize*len(r.buffer.columns))
	r.readers = make([]columnValueReader, len(r.buffer.columns))
	for i, c := range r.buffer.columns {
		r.readers[i].buffer = buffer[:0:columnBufferSize]
		r.readers[i].reader = c.Values()
		buffer = buffer[columnBufferSize:]
	}
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

func (r *bufferRowReader) Schema() *Schema {
	return r.buffer.schema
}

var (
	_ RowReaderWithSchema = (*bufferRowReader)(nil)
	_ RowWriterTo         = (*bufferRowReader)(nil)
)

type columnValueReader struct {
	buffer []Value
	offset uint
	reader ValueReader
}

func (r *columnValueReader) readMoreValues() error {
	n, err := r.reader.ReadValues(r.buffer[:cap(r.buffer)])
	if n == 0 {
		return err
	}
	r.buffer = r.buffer[:n]
	r.offset = 0
	return nil
}

type columnReadRowFunc func(Row, int8, []columnValueReader) (Row, error)

func columnReadRowFuncOf(node Node, columnIndex int, repetitionDepth int8) (int, columnReadRowFunc) {
	var read columnReadRowFunc

	if node.Repeated() {
		repetitionDepth++
	}

	if isLeaf(node) {
		columnIndex, read = columnReadRowFuncOfLeaf(node, columnIndex, repetitionDepth)
	} else {
		columnIndex, read = columnReadRowFuncOfGroup(node, columnIndex, repetitionDepth)
	}

	if node.Repeated() {
		read = columnReadRowFuncOfRepeated(node, repetitionDepth, read)
	}

	return columnIndex, read
}

//go:noinline
func columnReadRowFuncOfRepeated(node Node, repetitionDepth int8, read columnReadRowFunc) columnReadRowFunc {
	return func(row Row, repetitionLevel int8, columns []columnValueReader) (Row, error) {
		var err error

		for {
			n := len(row)

			if row, err = read(row, repetitionLevel, columns); err != nil {
				return row, err
			}
			if n == len(row) {
				return row, nil
			}

			repetitionLevel = repetitionDepth
		}
	}
}

//go:noinline
func columnReadRowFuncOfGroup(node Node, columnIndex int, repetitionDepth int8) (int, columnReadRowFunc) {
	names := node.ChildNames()
	if len(names) == 1 {
		return columnReadRowFuncOf(node.ChildByName(names[0]), columnIndex, repetitionDepth)
	}

	group := make([]columnReadRowFunc, len(names))
	for i, name := range names {
		columnIndex, group[i] = columnReadRowFuncOf(node.ChildByName(name), columnIndex, repetitionDepth)
	}

	return columnIndex, func(row Row, repetitionLevel int8, columns []columnValueReader) (Row, error) {
		var err error

		for _, read := range group {
			if row, err = read(row, repetitionLevel, columns); err != nil {
				break
			}
		}

		return row, err
	}
}

//go:noinline
func columnReadRowFuncOfLeaf(node Node, columnIndex int, repetitionDepth int8) (int, columnReadRowFunc) {
	var read columnReadRowFunc
	var valueColumnIndex = ^int8(columnIndex)

	if repetitionDepth == 0 {
		read = func(row Row, _ int8, columns []columnValueReader) (Row, error) {
			col := &columns[columnIndex]

			for {
				if col.offset < uint(len(col.buffer)) {
					v := col.buffer[col.offset]
					v.columnIndex = valueColumnIndex
					row = append(row, v)
					col.offset++
					return row, nil
				}
				if err := col.readMoreValues(); err != nil {
					return row, err
				}
			}
		}
	} else {
		read = func(row Row, repetitionLevel int8, columns []columnValueReader) (Row, error) {
			col := &columns[columnIndex]

			for {
				if col.offset < uint(len(col.buffer)) {
					if v := col.buffer[col.offset]; v.repetitionLevel == repetitionLevel {
						v.columnIndex = valueColumnIndex
						col.offset++
						row = append(row, v)
					}
					return row, nil
				}
				if err := col.readMoreValues(); err != nil {
					if repetitionLevel > 0 && err == io.EOF {
						err = nil
					}
					return row, err
				}
			}
		}
	}

	return columnIndex + 1, read
}

var (
	_ sort.Interface = (*Buffer)(nil)
)
