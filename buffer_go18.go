package parquet

import (
	"reflect"
	"sort"
)

type GenericBuffer[T any] struct {
	base    Buffer
	write   bufferFunc[T]
	columns columnBufferWriter
}

type bufferFunc[T any] func(*GenericBuffer[T], []T) (int, error)

func NewGenericBuffer[T any](options ...RowGroupOption) *GenericBuffer[T] {
	config, err := NewRowGroupConfig(options...)
	if err != nil {
		panic(err)
	}

	var model T
	var t = reflect.TypeOf(model)
	if config.Schema == nil {
		config.Schema = schemaOf(t)
	}

	buf := &GenericBuffer[T]{
		base: Buffer{config: config},
	}
	buf.base.configure(config.Schema)
	buf.write = bufferFuncOf[T](config.Schema)
	buf.columns = columnBufferWriter{
		columns: buf.base.columns,
		values:  make([]Value, 0, defaultValueBufferSize),
	}
	return buf
}

func bufferFuncOf[T any](schema *Schema) bufferFunc[T] {
	var model T

	switch t := reflect.TypeOf(model); t.Kind() {
	case reflect.Interface, reflect.Map:
		return (*GenericBuffer[T]).writeRows

	case reflect.Struct:
		size := t.Size()
		writeRows := writeRowsFuncOf(t, schema, nil)
		return func(buf *GenericBuffer[T], rows []T) (n int, err error) {
			defer buf.columns.clear()
			err = writeRows(&buf.columns, makeArray(rows), size, 0, columnLevels{})
			if err == nil {
				n = len(rows)
			}
			return n, err
		}

	default:
		panic("cannot create buffer for values of type " + t.String())
	}
}

func (buf *GenericBuffer[T]) Size() int64 {
	return buf.base.Size()
}

func (buf *GenericBuffer[T]) NumRows() int64 {
	return buf.base.NumRows()
}

func (buf *GenericBuffer[T]) ColumnChunks() []ColumnChunk {
	return buf.base.ColumnChunks()
}

func (buf *GenericBuffer[T]) ColumnBuffers() []ColumnBuffer {
	return buf.base.ColumnBuffers()
}

func (buf *GenericBuffer[T]) SortingColumns() []SortingColumn {
	return buf.base.SortingColumns()
}

func (buf *GenericBuffer[T]) Len() int {
	return buf.base.Len()
}

func (buf *GenericBuffer[T]) Less(i, j int) bool {
	return buf.base.Less(i, j)
}

func (buf *GenericBuffer[T]) Swap(i, j int) {
	buf.base.Swap(i, j)
}

func (buf *GenericBuffer[T]) Reset() {
	buf.base.Reset()
}

func (buf *GenericBuffer[T]) Write(rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	return buf.write(buf, rows)
}

func (buf *GenericBuffer[T]) WriteRows(rows []Row) (int, error) {
	return buf.base.WriteRows(rows)
}

func (buf *GenericBuffer[T]) WriteRowGroup(rowGroup RowGroup) (int64, error) {
	return buf.base.WriteRowGroup(rowGroup)
}

func (buf *GenericBuffer[T]) Rows() Rows {
	return buf.base.Rows()
}

func (buf *GenericBuffer[T]) Schema() *Schema {
	return buf.base.Schema()
}

func (buf *GenericBuffer[T]) writeRows(rows []T) (int, error) {
	if cap(buf.base.rowbuf) < len(rows) {
		buf.base.rowbuf = make([]Row, len(rows))
	} else {
		buf.base.rowbuf = buf.base.rowbuf[:len(rows)]
	}
	defer clearRows(buf.base.rowbuf)

	schema := buf.base.Schema()
	for i := range rows {
		buf.base.rowbuf[i] = schema.Deconstruct(buf.base.rowbuf[i], &rows[i])
	}

	return buf.base.WriteRows(buf.base.rowbuf)
}

var (
	_ RowGroup       = (*GenericBuffer[any])(nil)
	_ RowGroupWriter = (*GenericBuffer[any])(nil)
	_ sort.Interface = (*GenericBuffer[any])(nil)

	_ RowGroup       = (*GenericBuffer[struct{}])(nil)
	_ RowGroupWriter = (*GenericBuffer[struct{}])(nil)
	_ sort.Interface = (*GenericBuffer[struct{}])(nil)

	_ RowGroup       = (*GenericBuffer[map[struct{}]struct{}])(nil)
	_ RowGroupWriter = (*GenericBuffer[map[struct{}]struct{}])(nil)
	_ sort.Interface = (*GenericBuffer[map[struct{}]struct{}])(nil)
)
