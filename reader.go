package parquet

import (
	"fmt"
	"io"
	"reflect"

	"github.com/segmentio/parquet/encoding"
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
	file    *File
	schema  *Schema
	seen    reflect.Type
	columns []*columnChunkReader
	buffer  []Value
	values  []Value
	conv    ConvertFunc
	read    columnReadFunc
}

// NewReader constructs a parquet reader reading rows from the given
// io.ReaderAt.
//
// In order to read parquet rows, the io.ReaderAt must be converted to a
// parquet.File. If r is already a parquet.File it is used directly; otherwise,
// the io.ReaderAt value is expected to either have a `Size() int64` method or
// implement io.Seeker in order to determine its size.
func NewReader(r io.ReaderAt, options ...ReaderOption) *Reader {
	f, _ := r.(*File)
	if f == nil {
		n, err := sizeOf(r)
		if err != nil {
			panic(err)
		}
		if f, err = OpenFile(r, n); err != nil {
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
	columns := make([]*columnChunkReader, 0, numColumnsOf(root))
	root.forEachLeaf(func(column *Column) {
		columns = append(columns, newColumnChunkReader(column, config))
	})

	return &Reader{
		file:    f,
		schema:  NewSchema(root.Name(), root),
		columns: columns,
		values:  make([]Value, 0, len(columns)),
		read:    columnReadFuncOf(root, columns),
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
	for _, c := range r.columns {
		c.reset()
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
			if nodesAreEqual(schema, r.schema) {
				r.conv = nil
			} else {
				conv, err := Convert(schema, r.schema)
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
			r.schema = schema
			r.seen = rowType
		}
	}

	r.values, err = r.ReadRow(r.values[:0])
	if err != nil {
		return err
	}

	values := r.values
	if r.conv != nil {
		r.buffer, err = r.conv(r.buffer[:0], values)
		if err != nil {
			return fmt.Errorf("cannot convert parquet row to go value of type %T: %w", row, err)
		}
		values = r.buffer
	}

	return r.schema.Reconstruct(row, values)
}

// ReadRow reads the next row from r and appends in to the given Row buffer.
//
// The returned values are laid out in the order expected by the
// parquet.(*Schema).Reconstrct method.
//
// The method returns io.EOF when no more rows can be read from r.
func (r *Reader) ReadRow(buf Row) (Row, error) {
	n := len(buf)
	buf, err := r.read(buf, 0)
	if err == nil && len(buf) == n {
		err = io.EOF
	}
	return buf, err
}

type columnTreeReader interface {
	read(Row, int8) (Row, error)
}

type columnReadFunc func(Row, int8) (Row, error)

func columnReadFuncOf(column *Column, readers []*columnChunkReader) columnReadFunc {
	var reader columnReadFunc
	if column.NumChildren() == 0 {
		reader = columnReadFuncOfLeaf(column, readers)
	} else {
		reader = columnReadFuncOfGroup(column, readers)
	}
	if column.Repeated() {
		reader = columnReadFuncOfRepeated(column, reader)
	}
	return reader
}

//go:noinline
func columnReadFuncOfRepeated(column *Column, read columnReadFunc) columnReadFunc {
	repetitionLevel := column.MaxRepetitionLevel()
	return func(row Row, level int8) (Row, error) {
		var err error
		for {
			n := len(row)
			if row, err = read(row, level); err != nil {
				return row, err
			}
			if n == len(row) {
				return row, nil
			}
			level = repetitionLevel
		}
	}
}

//go:noinline
func columnReadFuncOfGroup(column *Column, readers []*columnChunkReader) columnReadFunc {
	children := column.Children()
	if len(children) == 1 {
		// Small optimization for a somewhat common case of groups with a single
		// column (like nested list elements for example); there is no need to
		// loop over the group of a single element, we can simply skip to calling
		// the inner read function.
		return columnReadFuncOf(children[0], readers)
	}
	group := make([]columnReadFunc, len(children))
	for i, child := range children {
		group[i] = columnReadFuncOf(child, readers)
	}
	return func(row Row, level int8) (Row, error) {
		var err error
		for _, read := range group {
			if row, err = read(row, level); err != nil {
				break
			}
		}
		return row, err
	}
}

//go:noinline
func columnReadFuncOfLeaf(column *Column, readers []*columnChunkReader) columnReadFunc {
	leaf := readers[column.Index()]

	if column.MaxRepetitionLevel() == 0 {
		return func(row Row, _ int8) (Row, error) {
			// Manually inline the buffered value read because the cast is too high
			// for the compiler in ReadValue. This gives a ~20% increase in throughput.
			if leaf.offset < uint(len(leaf.values)) {
				row = append(row, leaf.values[leaf.offset])
				leaf.offset++
				return row, nil
			}
			v, err := leaf.readValue()
			if err == nil {
				row = append(row, v)
			}
			return row, err
		}
	}

	return func(row Row, level int8) (Row, error) {
		var v Value
		var err error

		if leaf.peeked {
			v = leaf.cursor
		} else {
			if leaf.offset < uint(len(leaf.values)) {
				v = leaf.values[leaf.offset]
				leaf.offset++
			} else if v, err = leaf.readValue(); err != nil {
				if level > 0 && err == io.EOF {
					err = nil
				}
				return row, err
			}
		}

		if v.repetitionLevel == level {
			leaf.peeked = false
			leaf.cursor = Value{}
			row = append(row, v)
		} else {
			leaf.peeked = true
			leaf.cursor = v
		}

		return row, nil
	}
}

type columnChunkReader struct {
	bufferSize int
	column     *Column
	chunks     *ColumnChunks
	pages      *ColumnPages
	reader     *PageReader
	dictionary Dictionary
	numPages   int

	values []Value
	offset uint

	repetitions struct {
		decoder encoding.Decoder
	}
	definitions struct {
		decoder encoding.Decoder
	}
	page struct {
		typ     Type
		decoder encoding.Decoder
	}

	peeked bool
	cursor Value
}

func newColumnChunkReader(column *Column, config *ReaderConfig) *columnChunkReader {
	ccr := &columnChunkReader{
		bufferSize: config.PageBufferSize,
		column:     column,
		chunks:     column.Chunks(),
		values:     make([]Value, 0, 170),
	}

	maxRepetitionLevel := column.MaxRepetitionLevel()
	maxDefinitionLevel := column.MaxDefinitionLevel()

	if maxRepetitionLevel > 0 || maxDefinitionLevel > 0 {
		ccr.bufferSize /= 2
	}

	ccr.page.typ = column.Type()
	return ccr
}

func (ccr *columnChunkReader) reset() {
	if ccr.pages != nil {
		ccr.pages.close(io.EOF)
	}

	ccr.chunks.Seek(0)
	ccr.values = ccr.values[:0]
	ccr.offset = 0
	ccr.numPages = 0

	ccr.peeked = false
	ccr.cursor = Value{}
}

func (ccr *columnChunkReader) readValue() (Value, error) {
readNextValue:
	for {
		if ccr.offset < uint(len(ccr.values)) {
			v := ccr.values[ccr.offset]
			ccr.offset++
			return v, nil
		}
		if ccr.reader == nil {
			break
		}
		n, err := ccr.reader.ReadValues(ccr.values[:cap(ccr.values)])
		if n == 0 {
			if err == io.EOF {
				break
			}
			return Value{}, fmt.Errorf("%s: %w", join(ccr.column.Path()), err)
		}
		ccr.values = ccr.values[:n]
		ccr.offset = 0
	}

readNextPage:
	if ccr.pages != nil {
		if !ccr.pages.Next() {
			// Here the page count needs to be reset because we are changing the
			// column chunk and we may have to read another dictionary page.
			ccr.numPages = 0
			if ccr.dictionary != nil {
				ccr.dictionary.Reset()
			}
		} else {
			var err error

			switch header := ccr.pages.PageHeader().(type) {
			case DictionaryPageHeader:
				if ccr.numPages != 0 {
					err = fmt.Errorf("the dictionary must be in the first page but one was found after reading %d pages", ccr.numPages)
				} else {
					err = ccr.readDictionaryPage(header)
				}
				if err != nil {
					return Value{}, err
				}
				goto readNextPage
			case DataPageHeader:
				ccr.readDataPage(header)
				goto readNextValue
			default:
				return Value{}, fmt.Errorf("unsupported page header type: %#v", header)
			}
		}
	}

	if !ccr.chunks.Next() {
		return Value{}, io.EOF
	}

	ccr.pages = ccr.chunks.PagesTo(ccr.pages)
	goto readNextPage
}

func (ccr *columnChunkReader) readDictionaryPage(header DictionaryPageHeader) error {
	if ccr.dictionary == nil {
		ccr.dictionary = ccr.page.typ.NewDictionary(ccr.bufferSize)
	} else {
		ccr.dictionary.Reset()
	}
	decoder := header.Encoding().NewDecoder(ccr.pages.PageData())
	if err := ccr.dictionary.ReadFrom(decoder); err != nil {
		return err
	}
	ccr.page.typ = ccr.dictionary.Type()
	ccr.numPages++
	return nil
}

func (ccr *columnChunkReader) readDataPage(header DataPageHeader) {
	ccr.repetitions.decoder = makeDecoder(ccr.repetitions.decoder, header.RepetitionLevelEncoding(), ccr.pages.RepetitionLevels())
	ccr.definitions.decoder = makeDecoder(ccr.definitions.decoder, header.DefinitionLevelEncoding(), ccr.pages.DefinitionLevels())
	ccr.page.decoder = makeDecoder(ccr.page.decoder, header.Encoding(), ccr.pages.PageData())

	if ccr.reader == nil {
		ccr.reader = NewPageReader(
			ccr.page.typ,
			ccr.column.MaxRepetitionLevel(),
			ccr.column.MaxDefinitionLevel(),
			ccr.column.Index(),
			ccr.bufferSize,
		)
	}

	ccr.reader.Reset(header.NumValues(), ccr.repetitions.decoder, ccr.definitions.decoder, ccr.page.decoder)
	ccr.numPages++
}

func makeDecoder(decoder encoding.Decoder, encoding encoding.Encoding, input io.Reader) encoding.Decoder {
	if decoder == nil || encoding.Encoding() != decoder.Encoding() {
		decoder = encoding.NewDecoder(input)
	} else {
		decoder.Reset(input)
	}
	return decoder
}

var (
	_ RowReader = (*Reader)(nil)
)
