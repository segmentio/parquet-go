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
	columns    []columnValueReader
	buffer     []Value
	values     []Value
	conv       Conversion
	readRow    columnReadRowFunc
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

	config := &ReaderConfig{
		PageBufferSize: DefaultPageBufferSize,
	}
	config.Apply(options...)
	if err := config.Validate(); err != nil {
		panic(err)
	}

	root := f.Root()

	columnPages := make([]multiReusablePageReader, numColumnsOf(root))
	columnIndex := 0
	root.forEachLeaf(func(col *Column) {
		col.setPagesOn(&columnPages[columnIndex])
		columnIndex++
	})
	columns := makeColumnValueReaders(len(columnPages), func(i int) PageReader {
		return &columnPages[i]
	})

	schema := NewSchema(root.Name(), root)
	return &Reader{
		file:       f,
		fileSchema: schema,
		readSchema: schema,
		columns:    columns,
		values:     make([]Value, 0, len(columns)),
		readRow:    schema.readRow,
	}
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
	for i := range r.columns {
		r.columns[i].reset()
	}
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
func (r *Reader) ReadRow(buf Row) (Row, error) {
	n := len(buf)
	buf, err := r.readRow(buf, 0, r.columns)
	if err == nil && len(buf) == n {
		err = io.EOF
	}
	return buf, err
}

// Schema returns the schema of rows read by r.
func (r *Reader) Schema() *Schema { return r.fileSchema }

type pageAndValueWriter interface {
	PageWriter
	ValueWriter
}

type columnValueReader struct {
	// These two fields must be configured to initialize the reader.
	reader PageReader // reader of column pages
	buffer []Value    // buffer holding values read from the page
	// The rest of the fields are used to managae the state of the reader as it
	// consumes values from the underlying pages.
	offset int         // offset of the next value in the buffer
	page   Page        // current page where values are being read from
	values ValueReader // reader for values from the current page
}

func makeColumnValueReaders(numColumns int, columnPagesOf func(int) PageReader) []columnValueReader {
	const columnBufferSize = defaultValueBufferSize
	buffer := make([]Value, columnBufferSize*numColumns)
	readers := make([]columnValueReader, numColumns)

	for i := 0; i < numColumns; i++ {
		readers[i].reader = columnPagesOf(i)
		readers[i].buffer = buffer[:0:columnBufferSize]
		buffer = buffer[columnBufferSize:]
	}

	return readers
}

func (r *columnValueReader) reset() {
	clearValues(r.buffer)
	r.buffer = r.buffer[:0]
	r.offset = 0
	r.page = nil
	// If the underlying type does not implement resusablePageReader the next
	// attempt to read values will be io.EOF because we set the pages to nil.
	if p, ok := r.reader.(reusablePageReader); ok {
		p.Reset()
	} else {
		r.reader = nil
	}
	r.values = nil
}

func (r *columnValueReader) buffered() int {
	return len(r.buffer) - r.offset
}

func (r *columnValueReader) readPage() (err error) {
	if r.page != nil {
		return nil
	}
	if r.reader == nil {
		return io.EOF
	}
	for {
		p, err := r.reader.ReadPage()
		if err != nil {
			return err
		}
		if p.NumValues() > 0 {
			r.page = p
			return nil
		}
	}
}

func (r *columnValueReader) readValues() error {
	for {
		err := r.readValuesFromCurrentPage()
		if err == nil || err != io.EOF {
			return err
		}
		if err := r.readPage(); err != nil {
			return err
		}
	}
}

func (r *columnValueReader) readValuesFromCurrentPage() error {
	if r.offset < len(r.buffer) {
		return nil
	}
	if r.page == nil {
		return io.EOF
	}
	if r.values == nil {
		r.values = r.page.Values()
	}
	n, err := r.values.ReadValues(r.buffer[:cap(r.buffer)])
	if err != nil && err == io.EOF {
		r.page, r.values = nil, nil
	}
	if n > 0 {
		err = nil
	}
	r.buffer = r.buffer[:n]
	r.offset = 0
	return err
}

func (r *columnValueReader) writeBufferedRowsTo(w pageAndValueWriter, rowCount int64) (numRows int64, err error) {
	if rowCount == 0 {
		return 0, nil
	}

	for {
		for r.offset < len(r.buffer) {
			values := r.buffer[r.offset:]
			// We can only determine that the full row has been consumed if we
			// have more values in the buffer, and the next value is the start
			// of a new row. Otherwise, we have to load more values from the
			// page, which may yield EOF if all values have been consumed, in
			// which case we know that we have read the full row, and otherwise
			// we will enter this check again on the next loop iteration.
			if numRows == rowCount {
				if values[0].repetitionLevel == 0 {
					return numRows, nil
				}
				values, _ = splitRowValues(values)
			} else {
				values = limitRowValues(values, int(rowCount-numRows))
			}

			n, err := w.WriteValues(values)
			numRows += int64(countRowsOf(values[:n]))
			r.offset += n
			if err != nil {
				return numRows, err
			}
		}

		if err := r.readValuesFromCurrentPage(); err != nil {
			if err == io.EOF {
				err = nil
			}
			return numRows, err
		}
	}
}

func (r *columnValueReader) writeRowsTo(w pageAndValueWriter, limit int64) (numRows int64, err error) {
	for numRows < limit {
		if r.values != nil {
			n, err := r.writeBufferedRowsTo(w, numRows-limit)
			numRows += n
			if err != nil || numRows == limit {
				return numRows, err
			}
		}

		r.buffer = r.buffer[:0]
		r.offset = 0

		for numRows < limit {
			p, err := r.reader.ReadPage()
			if err != nil {
				return numRows, err
			}

			pageRows := int64(p.NumRows())
			// When the page is fully contained in the remaining range of rows
			// that we intend to copy, we can use an optimized pagge copy rather
			// than writing rows one at a time.
			//
			// Data pages v1 do not expose the number of rows available, which
			// means we cannot take the optimized page copy path in those cases.
			if pageRows == 0 || int64(pageRows) > limit {
				r.values = p.Values()
				err := r.readValuesFromCurrentPage()
				if err == nil {
					// More values have been buffered, break out of the inner loop
					// to go back to the beginning of the outer loop and write
					// buffered values to the output.
					break
				}
				if err == io.EOF {
					// The page contained no values? Unclear if this is valid but
					// we can handle it by reading the next page.
					r.values = nil
					continue
				}
				return numRows, err
			}

			if _, err := w.WritePage(p); err != nil {
				return numRows, err
			}

			numRows += pageRows
		}
	}
	return numRows, nil
}

type columnReadRowFunc func(Row, int8, []columnValueReader) (Row, error)

func columnReadRowFuncOf(node Node, columnIndex int, repetitionDepth int8) (int, columnReadRowFunc) {
	var read columnReadRowFunc

	if node.Repeated() {
		repetitionDepth++
	}

	if isLeaf(node) {
		columnIndex, read = columnReadRowFuncOfLeaf(columnIndex, repetitionDepth)
	} else {
		columnIndex, read = columnReadRowFuncOfGroup(node, columnIndex, repetitionDepth)
	}

	if node.Repeated() {
		read = columnReadRowFuncOfRepeated(read, repetitionDepth)
	}

	return columnIndex, read
}

//go:noinline
func columnReadRowFuncOfRepeated(read columnReadRowFunc, repetitionDepth int8) columnReadRowFunc {
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
		// Small optimization for a somewhat common case of groups with a single
		// column (like nested list elements for example); there is no need to
		// loop over the group of a single element, we can simply skip to calling
		// the inner read function.
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
func columnReadRowFuncOfLeaf(columnIndex int, repetitionDepth int8) (int, columnReadRowFunc) {
	var read columnReadRowFunc

	if repetitionDepth == 0 {
		read = func(row Row, _ int8, columns []columnValueReader) (Row, error) {
			col := &columns[columnIndex]

			for {
				if col.offset < len(col.buffer) {
					row = append(row, col.buffer[col.offset])
					col.offset++
					return row, nil
				}
				if err := col.readValues(); err != nil {
					return row, err
				}
			}
		}
	} else {
		read = func(row Row, repetitionLevel int8, columns []columnValueReader) (Row, error) {
			col := &columns[columnIndex]

			for {
				if col.offset < len(col.buffer) {
					if col.buffer[col.offset].repetitionLevel == repetitionLevel {
						row = append(row, col.buffer[col.offset])
						col.offset++
					}
					return row, nil
				}
				if err := col.readValues(); err != nil {
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
	_ RowReaderWithSchema = (*Reader)(nil)
)
