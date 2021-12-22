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
//		err := reader.ReadRow(&row)
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
	values  []Value
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
	for i := range r.values {
		r.values[i] = Value{}
	}
}

// ReadRow reads the next row from r. The type of the row must match the schema
// of the underlying parquet file or an error will be returned.
func (r *Reader) ReadRow(row interface{}) (err error) {
	if rowType := dereference(reflect.TypeOf(row)); rowType.Kind() == reflect.Struct {
		if r.seen == nil || r.seen != rowType {
			schema := namedSchemaOf(r.schema.Name(), rowType)
			if !Match(schema, r.schema) {
				return fmt.Errorf("cannot read parquet row into go value of type %T: schema mismatch", row)
			}
			// Replace the schema because the one created from the go type will be
			// optimized to decode into struct values.
			r.schema = schema
			r.seen = rowType
		}
	}
	r.values, err = r.read(r.values[:0], 0)
	if err != nil {
		return err
	}
	if len(r.values) == 0 {
		return io.EOF
	}
	return r.schema.Reconstruct(row, r.values)
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
	group := make([]columnReadFunc, len(children))
	for i, child := range children {
		group[i] = columnReadFuncOf(child, readers)
	}
	return func(row Row, level int8) (Row, error) {
		var err error
		for _, read := range group {
			if row, err = read(row, level); err != nil {
				err = columnReadError(column, err)
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
			v, err := leaf.readValue()
			if err != nil {
				return row, columnReadError(column, err)
			}
			return append(row, v), nil
		}
	}

	return func(row Row, level int8) (Row, error) {
		v, err := leaf.peekValue()
		if err != nil {
			if level > 0 && err == io.EOF {
				err = nil
			} else {
				err = columnReadError(column, err)
			}
			return row, err
		}
		if v.repetitionLevel == level {
			leaf.nextValue()
			row = append(row, v)
		}
		return row, nil
	}
}

func columnReadError(col *Column, err error) error {
	switch err {
	case nil, io.EOF:
		return err
	default:
		return fmt.Errorf("%s → %w", col.Name(), err)
	}
}

type columnChunkReader struct {
	bufferSize int
	column     *Column
	chunks     *ColumnChunks
	pages      *ColumnPages
	reader     *DataPageReader
	values     bufferedValueReader
	dictionary Dictionary
	numPages   int

	page struct {
		decoder encoding.Decoder
		reader  PageReader
	}
	repetition struct {
		decoder encoding.Decoder
	}
	definition struct {
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
		values: bufferedValueReader{
			buffer: make([]Value, 0, 170),
		},
	}

	maxRepetitionLevel := column.MaxRepetitionLevel()
	maxDefinitionLevel := column.MaxDefinitionLevel()

	if maxRepetitionLevel > 0 || maxDefinitionLevel > 0 {
		ccr.bufferSize /= 2
	}

	return ccr
}

func (ccr *columnChunkReader) reset() {
	ccr.chunks.Seek(0)
	ccr.pages = nil
	ccr.reader = nil
	ccr.values.Reset(nil)
	ccr.dictionary = nil
	ccr.numPages = 0
	ccr.peeked = false
	ccr.cursor = Value{}
}

func (ccr *columnChunkReader) peekValue() (Value, error) {
	if ccr.peeked {
		return ccr.cursor, nil
	}

	v, err := ccr.readValue()
	if err != nil {
		return v, err
	}

	ccr.peeked = true
	ccr.cursor = v
	return v, nil
}

func (ccr *columnChunkReader) nextValue() {
	ccr.peeked = false
	ccr.cursor = Value{}
}

func (ccr *columnChunkReader) readValue() (Value, error) {
readNextValue:
	// Manually inline the buffered value read because the cast is too high
	// for the compiler in ReadValue. This gives a ~10% increase in throughput.
	if ccr.values.offset < uint(len(ccr.values.buffer)) {
		v := ccr.values.buffer[ccr.values.offset]
		ccr.values.offset++
		return v, nil
	}

	v, err := ccr.values.ReadValue()
	if err != nil && err == io.EOF {
		goto readNextPage
	}
	return v, err

readNextPage:
	if ccr.pages != nil {
		if !ccr.pages.Next() {
			ccr.pages = nil
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

	ccr.pages = ccr.chunks.Pages()
	goto readNextPage
}

func (ccr *columnChunkReader) readDictionaryPage(header DictionaryPageHeader) error {
	ccr.dictionary = ccr.column.Type().NewDictionary(0)
	if err := ccr.dictionary.ReadFrom(
		header.Encoding().NewDecoder(ccr.pages.PageData()),
	); err != nil {
		return err
	}
	ccr.numPages++
	return nil
}

func (ccr *columnChunkReader) readDataPage(header DataPageHeader) {
	ccr.page.decoder = resetDecoder(ccr.page.decoder, header.Encoding(), ccr.pages.PageData())

	if ccr.page.reader != nil {
		ccr.page.reader.Reset(ccr.page.decoder)
	} else {
		if ccr.dictionary != nil {
			ccr.page.reader = NewIndexedPageReader(ccr.page.decoder, ccr.bufferSize, ccr.dictionary)
		} else {
			ccr.page.reader = ccr.column.Type().NewPageReader(ccr.page.decoder, ccr.bufferSize)
		}
	}

	maxRepetitionLevel := ccr.column.MaxRepetitionLevel()
	maxDefinitionLevel := ccr.column.MaxDefinitionLevel()

	if maxRepetitionLevel > 0 {
		ccr.repetition.decoder = resetDecoder(ccr.repetition.decoder, header.RepetitionLevelEncoding(), ccr.pages.RepetitionLevels())
	}
	if maxDefinitionLevel > 0 {
		ccr.definition.decoder = resetDecoder(ccr.definition.decoder, header.DefinitionLevelEncoding(), ccr.pages.DefinitionLevels())
	}

	if ccr.reader != nil {
		ccr.reader.Reset(ccr.repetition.decoder, ccr.definition.decoder, header.NumValues(), ccr.page.reader)
	} else {
		ccr.reader = NewDataPageReader(
			ccr.repetition.decoder,
			ccr.definition.decoder,
			header.NumValues(),
			ccr.page.reader,
			maxRepetitionLevel,
			maxDefinitionLevel,
			ccr.column.Index(),
			ccr.bufferSize,
		)
	}

	ccr.values.Reset(ccr.reader)
	ccr.numPages++
}

func resetDecoder(decoder encoding.Decoder, encoding encoding.Encoding, input io.Reader) encoding.Decoder {
	if decoder == nil || encoding.Encoding() != decoder.Encoding() {
		decoder = encoding.NewDecoder(input)
	} else {
		decoder.Reset(input)
	}
	return decoder
}
