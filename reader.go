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
	reader  columnTreeReader
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

	_, reader := columnTreeReaderOf(0, root, columns)
	return &Reader{
		file:    f,
		schema:  NewSchema(root.Name(), root),
		columns: columns,
		values:  make([]Value, 0, len(columns)),
		reader:  reader,
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
func (r *Reader) ReadRow(row interface{}) error {
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

	var err error
	r.reader.next()
	r.values, err = r.reader.read(r.values[:0])
	//fmt.Printf("row = %+v (%v)\n", r.values, err)
	if err != nil {
		return err
	}
	return r.schema.Reconstruct(row, r.values)
}

type columnTreeReader interface {
	next()
	read(Row) (Row, error)
}

func columnTreeReaderOf(columnIndex int, column *Column, readers []*columnChunkReader) (int, columnTreeReader) {
	if column.NumChildren() == 0 {
		repetitionLevel := column.MaxRepetitionLevel()
		if repetitionLevel == 0 {
			return columnIndex + 1, &leafReader{
				column: readers[columnIndex],
			}
		} else {
			return columnIndex + 1, &repeatedLeafReader{
				repetitionLevel: repetitionLevel,
				column:          readers[columnIndex],
			}
		}
	}
	children := make([]columnTreeReader, column.NumChildren())
	for i, child := range column.Children() {
		columnIndex, children[i] = columnTreeReaderOf(columnIndex, child, readers)
	}
	return columnIndex, &repeatedGroupReader{
		children: children,
		done:     make([]bool, len(children)),
	}
}

type leafReader struct {
	done   bool
	column *columnChunkReader
}

func (leaf *leafReader) next() {
	leaf.done = false
}

func (leaf *leafReader) read(row Row) (Row, error) {
	if !leaf.done {
		leaf.done = true
		v, err := leaf.column.readValue()
		if err != nil {
			return row, err
		}
		row = append(row, v)
	}
	return row, nil
}

type repeatedLeafReader struct {
	init            bool
	done            bool
	repetitionLevel int8
	column          *columnChunkReader
}

func (leaf *repeatedLeafReader) next() {
	leaf.init = false
	leaf.done = false
}

func (leaf *repeatedLeafReader) read(row Row) (Row, error) {
	if leaf.done {
		return row, nil
	}

	if !leaf.init { // v.repetitionLevel <= leaf.repetitionLevel
		leaf.init = true
		v, err := leaf.column.peekValue()
		if err != nil {
			return row, err
		}
		leaf.column.nextValue()
		return append(row, v), nil
	}

	v, err := leaf.column.peekValue()
	if err != nil {
		if err == io.EOF {
			leaf.done, err = true, nil
		}
		return row, err
	}
	if v.repetitionLevel != leaf.repetitionLevel {
		leaf.done = true
		return row, nil
	}
	leaf.column.nextValue()
	return append(row, v), nil
}

type groupReader struct {
	done     bool
	children []columnTreeReader
}

func (group *groupReader) next() {
	group.done = false
}

func (group *groupReader) read(row Row) (Row, error) {
	var err error
	for _, child := range group.children {
		if row, err = child.read(row); err != nil {
			break
		}
	}
	return row, err
}

type repeatedGroupReader struct {
	children []columnTreeReader
	done     []bool
}

func (group *repeatedGroupReader) next() {
	for _, r := range group.children {
		r.next()
	}
	for i := range group.done {
		group.done[i] = false
	}
}

func (group *repeatedGroupReader) read(row Row) (Row, error) {
	readers := group.children
	done := group.done[:len(readers)]
	eof := 0

	for eof < len(readers) {
		eof = 0
		for i, r := range readers {
			if done[i] {
				eof++
			} else {
				var err error
				var n = len(row)
				if row, err = r.read(row); err != nil {
					return row, err
				}
				done[i] = len(row) == n
			}
		}
	}

	return row, nil
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
		return Value{}, err
	}

	ccr.peeked = true
	ccr.cursor = v
	return ccr.cursor, nil
}

func (ccr *columnChunkReader) nextValue() {
	ccr.peeked = false
	ccr.cursor = Value{}
}

func (ccr *columnChunkReader) readValue() (Value, error) {
readNextValue:
	if ccr.reader != nil {
		v, err := ccr.values.ReadValue()
		if err != nil {
			if err == io.EOF {
				goto readNextPage
			}
		}
		return v, err
	}

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
