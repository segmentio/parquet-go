//go:build go1.18

package parquet

import (
	"fmt"
	"io"
	"reflect"
)

// GenericReader is similar to a Reader but uses a type parameter to define the
// Go type representing the schema of rows being read.
//
// See GenericWriter for details about the benefits over the classic Reader API.
type GenericReader[T any] struct {
	base Reader
	read readFunc[T]
}

// NewGenericReader is like NewReader but returns GenericReader[T] suited to write
// rows of Go type T.
//
// The type parameter T should be a map, struct, or any. Any other types will
// cause a panic at runtime. Type checking is a lot more effective when the
// generic parameter is a struct type, using map and interface types is somewhat
// similar to using a Writer.
//
// If the option list may explicitly declare a schema, it must be compatible
// with the schema generated from T.
//
// On an error such as an invalid parquet input, NewGenericReader will panic. If
// it would be preferable to inspect the error instead, use NewGenericReaderOrError
func NewGenericReader[T any](input io.ReaderAt, options ...ReaderOption) *GenericReader[T] {
	r, err := NewGenericReaderOrError[T](input, options...)
	if err != nil {
		panic(err)
	}

	return r
}

// NewGenericReaderOrError is like NewReader but returns GenericReader[T] suited to write
// rows of Go type T.
//
// The type parameter T should be a map, struct, or any. Any other types will
// cause a panic at runtime. Type checking is a lot more effective when the
// generic parameter is a struct type, using map and interface types is somewhat
// similar to using a Writer.
//
// If the option list may explicitly declare a schema, it must be compatible
// with the schema generated from T.
func NewGenericReaderOrError[T any](input io.ReaderAt, options ...ReaderOption) (*GenericReader[T], error) {
	c, err := NewReaderConfig(options...)
	if err != nil {
		return nil, err
	}

	f, err := openFile(input)
	if err != nil {
		return nil, err
	}

	rowGroup := fileRowGroupOf(f)

	t := typeOf[T]()
	if c.Schema == nil {
		if t == nil {
			c.Schema = rowGroup.Schema()
		} else {
			c.Schema = schemaOf(dereference(t))
		}
	}

	r := &GenericReader[T]{
		base: Reader{
			file: reader{
				schema:   c.Schema,
				rowGroup: rowGroup,
			},
		},
	}

	if !nodesAreEqual(c.Schema, f.schema) {
		r.base.file.rowGroup, err = convertRowGroupTo(r.base.file.rowGroup, c.Schema)
		if err != nil {
			_ = r.Close()
			return nil, err
		}
	}

	r.base.read.init(r.base.file.schema, r.base.file.rowGroup)

	r.read, err = readFuncOf[T](t, r.base.file.schema)
	if err != nil {
		_ = r.Close()
		return nil, err
	}

	return r, nil
}

// NewGenericRowGroupReader constructs a new GemericReader which reads rows from
// the RowGroup passed as an argument. This function panics if it encounters any error;
// use NewGenericRowGroupReaderOrError to be able to handle failures more gracefully
func NewGenericRowGroupReader[T any](rowGroup RowGroup, options ...ReaderOption) *GenericReader[T] {
	r, err := NewGenericRowGroupReaderOrError[T](rowGroup, options...)
	if err != nil {
		panic(err)
	}

	return r
}

// NewGenericRowGroupReaderOrError constructs a new GemericReader which reads rows
// from the RowGrup passed as an argument.
func NewGenericRowGroupReaderOrError[T any](rowGroup RowGroup, options ...ReaderOption) (*GenericReader[T], error) {
	c, err := NewReaderConfig(options...)
	if err != nil {
		return nil, err
	}

	t := typeOf[T]()
	if c.Schema == nil {
		if t == nil {
			c.Schema = rowGroup.Schema()
		} else {
			c.Schema = schemaOf(dereference(t))
		}
	}

	r := &GenericReader[T]{
		base: Reader{
			file: reader{
				schema:   c.Schema,
				rowGroup: rowGroup,
			},
		},
	}

	if !nodesAreEqual(c.Schema, rowGroup.Schema()) {
		r.base.file.rowGroup, err = convertRowGroupTo(r.base.file.rowGroup, c.Schema)
		if err != nil {
			return nil, err
		}
	}

	r.base.read.init(r.base.file.schema, r.base.file.rowGroup)

	r.read, err = readFuncOf[T](t, r.base.file.schema)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *GenericReader[T]) Reset() {
	r.base.Reset()
}

func (r *GenericReader[T]) Read(rows []T) (int, error) {
	return r.read(r, rows)
}

func (r *GenericReader[T]) ReadRows(rows []Row) (int, error) {
	return r.base.ReadRows(rows)
}

func (r *GenericReader[T]) Schema() *Schema {
	return r.base.Schema()
}

func (r *GenericReader[T]) NumRows() int64 {
	return r.base.NumRows()
}

func (r *GenericReader[T]) SeekToRow(rowIndex int64) error {
	return r.base.SeekToRow(rowIndex)
}

func (r *GenericReader[T]) Close() error {
	return r.base.Close()
}

func (r *GenericReader[T]) readRows(rows []T) (int, error) {
	if cap(r.base.rowbuf) < len(rows) {
		r.base.rowbuf = make([]Row, len(rows))
	} else {
		r.base.rowbuf = r.base.rowbuf[:len(rows)]
	}

	n, err := r.base.ReadRows(r.base.rowbuf)
	if n > 0 {
		schema := r.base.Schema()

		for i, row := range r.base.rowbuf[:n] {
			if err := schema.Reconstruct(&rows[i], row); err != nil {
				return i, err
			}
		}
	}
	return n, err
}

var (
	_ Rows                = (*GenericReader[any])(nil)
	_ RowReaderWithSchema = (*Reader)(nil)

	_ Rows                = (*GenericReader[struct{}])(nil)
	_ RowReaderWithSchema = (*GenericReader[struct{}])(nil)

	_ Rows                = (*GenericReader[map[struct{}]struct{}])(nil)
	_ RowReaderWithSchema = (*GenericReader[map[struct{}]struct{}])(nil)
)

type readFunc[T any] func(*GenericReader[T], []T) (int, error)

func readFuncOf[T any](t reflect.Type, schema *Schema) (readFunc[T], error) {
	if t == nil {
		return (*GenericReader[T]).readRows, nil
	}

	switch t.Kind() {
	case reflect.Interface, reflect.Map:
		return (*GenericReader[T]).readRows, nil

	case reflect.Struct:
		return (*GenericReader[T]).readRows, nil

	case reflect.Pointer:
		if e := t.Elem(); e.Kind() == reflect.Struct {
			return (*GenericReader[T]).readRows, nil
		}
	}

	return nil, fmt.Errorf("cannot create reader for values of type %s", t.String())
}
