package parquet

import (
	"fmt"
	"io"
	"reflect"
)

// A Reader reads Go values from parquet files.
//
// This example showcases a typical use of parquet readers:
//
//	reader := parquet.NewReader(file)
//	rows := []RowType{}
//	for {
//		row := RowType{}
//		err := reader.Read(&row)
//		if err != nil {
//			if err == io.EOF {
//				break
//			}
//			...
//		}
//		rows = append(rows, row)
//	}
//
//
type Reader struct {
	file       *File
	fileSchema *Schema
	readSchema *Schema
	seen       reflect.Type
	rowGroup   RowGroup
	rows       Rows
	buffer     []Value
	values     []Value
	conv       Conversion
}

// NewReader constructs a parquet reader reading rows from the given
// io.ReaderAt.
//
// In order to read parquet rows, the io.ReaderAt must be converted to a
// parquet.File. If r is already a parquet.File it is used directly; otherwise,
// the io.ReaderAt value is expected to either have a `Size() int64` method or
// implement io.Seeker in order to determine its size.
//
// The function panics if the reader configuration is invalid. Programs that
// cannot guarantee the validity of the options passed to NewReader should
// construct the reader configuration independently prior to calling this
// function:
//
//	config, err := parquet.NewReaderConfig(options...)
//	if err != nil {
//		// handle the configuration error
//		...
//	} else {
//		// this call to create a reader is guaranteed not to panic
//		reader := parquet.NewReader(input, config)
//		...
//	}
//
func NewReader(input io.ReaderAt, options ...ReaderOption) *Reader {
	f, _ := input.(*File)
	if f == nil {
		n, err := sizeOf(input)
		if err != nil {
			panic(err)
		}
		if f, err = OpenFile(input, n); err != nil {
			panic(err)
		}
	}

	_, err := NewReaderConfig(options...)
	if err != nil {
		panic(err)
	}

	column := f.Root()
	schema := NewSchema(column.Name(), column)

	r := &Reader{
		file:       f,
		fileSchema: schema,
		readSchema: schema,
	}

	switch n := f.NumRowGroups(); n {
	case 0:
		r.rowGroup = newEmptyRowGroup(schema)
	case 1:
		r.rowGroup = f.RowGroup(0)
	default:
		rowGroups := make([]RowGroup, n)
		for i := range rowGroups {
			rowGroups[i] = f.RowGroup(i)
		}
		// TODO: should we attempt to merge the row groups via MergeRowGroups to
		// preserve the global order of sorting columns within the file?
		r.rowGroup = concat(schema, rowGroups)
	}
	return r
}

func sizeOf(r io.ReaderAt) (int64, error) {
	switch f := r.(type) {
	case interface{ Size() int64 }:
		return f.Size(), nil
	case io.Seeker:
		off, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		end, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		_, err = f.Seek(off, io.SeekStart)
		return end, err
	default:
		return 0, fmt.Errorf("cannot determine length of %T", r)
	}
}

// Reset repositions the reader at the beginning of the underlying parquet file.
func (r *Reader) Reset() {
	r.rows = nil // TODO: can we make the RowReader reusable?
	clearValues(r.buffer)
	clearValues(r.values)
}

// Read reads the next row from r. The type of the row must match the schema
// of the underlying parquet file or an error will be returned.
//
// The method returns io.EOF when no more rows can be read from r.
func (r *Reader) Read(row interface{}) (err error) {
	if rowType := dereference(reflect.TypeOf(row)); rowType.Kind() == reflect.Struct {
		if r.seen == nil || r.seen != rowType {
			schema := schemaOf(rowType)
			if nodesAreEqual(schema, r.readSchema) {
				r.conv = nil
			} else {
				conv, err := Convert(schema, r.readSchema)
				if err != nil {
					return fmt.Errorf("cannot read parquet row into go value of type %T: %w", row, err)
				}
				if r.buffer == nil {
					r.buffer = make([]Value, 0, cap(r.values))
				}
				r.conv = conv
			}
			// Replace the schema because the one created from the go type will be
			// optimized to decode into struct values.
			r.readSchema = schema
			r.seen = rowType
		}
	}

	r.values, err = r.ReadRow(r.values[:0])
	if err != nil {
		return err
	}

	values := r.values
	if r.conv != nil {
		r.buffer, err = r.conv.Convert(r.buffer[:0], values)
		if err != nil {
			return fmt.Errorf("cannot convert parquet row to go value of type %T: %w", row, err)
		}
		values = r.buffer
	}

	return r.readSchema.Reconstruct(row, values)
}

// ReadRow reads the next row from r and appends in to the given Row buffer.
//
// The returned values are laid out in the order expected by the
// parquet.(*Schema).Reconstrct method.
//
// The method returns io.EOF when no more rows can be read from r.
func (r *Reader) ReadRow(row Row) (Row, error) {
	if r.rows == nil {
		r.rows = r.rowGroup.Rows()
	}
	n := len(row)
	row, err := r.rows.ReadRow(row)
	if err == nil && len(row) == n {
		err = io.EOF
	}
	return row, err
}

// Schema returns the schema of rows read by r.
func (r *Reader) Schema() *Schema { return r.fileSchema }

// NumRows returns the number of rows that can be read from r.
func (r *Reader) NumRows() int64 { return r.rowGroup.NumRows() }

// SeekToRow positions r at the given row index.
func (r *Reader) SeekToRow(rowIndex int64) error {
	if r.rows == nil {
		r.rows = r.rowGroup.Rows()
	}
	return r.rows.SeekToRow(rowIndex)
}

var (
	_ Rows = (*Reader)(nil)
)
