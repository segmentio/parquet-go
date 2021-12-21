package parquet

import (
	"fmt"
	"io"
	"reflect"

	"github.com/segmentio/parquet/encoding"
)

type Reader struct {
	file    *File
	schema  *Schema
	seen    reflect.Type
	columns []*columnChunkReader
	buffers [][]Value
	indexes []uint
	values  []Value
	err     error
	flatten flattenRowFunc
}

func NewReader(r io.ReaderAt, size int64, options ...ReaderOption) *Reader {
	f, err := OpenFile(r, size)
	if err != nil {
		return &Reader{err: err}
	}
	return NewFileReader(f, options...)
}

func NewFileReader(file *File, options ...ReaderOption) *Reader {
	config := &ReaderConfig{
		PageBufferSize: DefaultPageBufferSize,
	}
	config.Apply(options...)
	if err := config.Validate(); err != nil {
		return &Reader{err: err}
	}
	root := file.Root()
	columns := make([]*columnChunkReader, 0, numColumnsOf(root))
	root.forEachLeaf(func(column *Column) {
		columns = append(columns, newColumnChunkReader(column, config))
	})
	_, flatten := flattenRowFuncOf(0, root)
	return &Reader{
		file:    file,
		schema:  NewSchema(root.Name(), root),
		columns: columns,
		buffers: make([][]Value, len(columns)),
		indexes: make([]uint, len(columns)),
		values:  make([]Value, 0, len(columns)),
		flatten: flatten,
	}
}

func (r *Reader) Reset() {
	for _, c := range r.columns {
		c.reset()
	}
}

func (r *Reader) ReadRow(row interface{}) error {
	if r.err != nil {
		return r.err
	}

	if rowType := dereference(reflect.TypeOf(row)); rowType.Kind() == reflect.Struct && (r.seen == nil || r.seen != rowType) {
		schema := namedSchemaOf(r.schema.Name(), rowType)
		if !Match(schema, r.schema) {
			return fmt.Errorf("cannot read parquet row into go value of type %T: schema mismatch", row)
		}
		// Replace the schema because the one created from the go type will be
		// optimized to decode into struct values.
		r.schema = schema
		r.seen = rowType
	}

	var err error
	defer func() {
		for i, b := range r.buffers {
			for j := range b {
				b[j] = Value{}
			}
			r.buffers[i] = b[:0]
		}
		for i := range r.values {
			r.values[i] = Value{}
		}
		r.values = r.values[:0]
	}()

	for i, c := range r.columns {
		if r.buffers[i], err = c.readRow(r.buffers[i][:0]); err != nil {
			return err
		}
	}

	for i := range r.indexes {
		r.indexes[i] = 0
	}

	r.values, _ = r.flatten(r.values, r.indexes, r.buffers)
	return r.schema.Reconstruct(row, r.values)
}

type flattenRowFunc func(row Row, indexes []uint, buffers [][]Value) (Row, int)

func flattenRowFuncOf(columnIndex int, column *Column) (int, flattenRowFunc) {
	if column.Repeated() {
		if isLeaf(column) {
			return flattenRowFuncOfRepeatedLeaf(columnIndex, column)
		} else {
			return flattenRowFuncOfRepeatedGroup(columnIndex, column)
		}
	} else {
		if isLeaf(column) {
			return flattenRowFuncOfLeaf(columnIndex, column)
		} else {
			return flattenRowFuncOfGroup(columnIndex, column)
		}
	}
}

//go:noinline
func flattenRowFuncOfLeaf(columnIndex int, column *Column) (int, flattenRowFunc) {
	return columnIndex + 1, func(row Row, indexes []uint, buffers [][]Value) (Row, int) {
		i := indexes[columnIndex]
		b := buffers[columnIndex]

		if i < uint(len(b)) {
			row = append(row, b[i])
			indexes[columnIndex]++
		}

		return row, int(uint(len(b)) - indexes[columnIndex])
	}
}

//go:noinline
func flattenRowFuncOfRepeatedLeaf(columnIndex int, column *Column) (int, flattenRowFunc) {
	maxRepetitionLevel := column.MaxRepetitionLevel()
	return columnIndex + 1, func(row Row, indexes []uint, buffers [][]Value) (Row, int) {
		i := indexes[columnIndex]
		b := buffers[columnIndex]

		if i < uint(len(b)) {
			// repetition level = 0
			row = append(row, b[i])
			i++

			for i < uint(len(b)) && b[i].repetitionLevel == maxRepetitionLevel {
				row = append(row, b[i])
				i++
			}
		}

		indexes[columnIndex] = i
		return row, int(uint(len(b)) - i)
	}
}

//go:noinline
func flattenRowFuncOfGroup(columnIndex int, column *Column) (int, flattenRowFunc) {
	funcs, columnIndex := makeFlattenRowFuncOfGroup(columnIndex, column)
	return columnIndex, func(row Row, indexes []uint, buffers [][]Value) (Row, int) {
		remain, rem := 0, 0

		for _, f := range funcs {
			row, rem = f(row, indexes, buffers)
			remain += rem
		}

		return row, remain
	}
}

//go:noinline
func flattenRowFuncOfRepeatedGroup(columnIndex int, column *Column) (int, flattenRowFunc) {
	funcs, columnIndex := makeFlattenRowFuncOfGroup(columnIndex, column)
	return columnIndex, func(row Row, indexes []uint, buffers [][]Value) (Row, int) {
		for {
			remain, rem := 0, 0
			for _, f := range funcs {
				row, rem = f(row, indexes, buffers)
				remain += rem
			}
			if remain == 0 {
				return row, 0
			}
		}
	}
}

func makeFlattenRowFuncOfGroup(columnIndex int, column *Column) ([]flattenRowFunc, int) {
	children := column.Children()
	funcs := make([]flattenRowFunc, len(children))
	for i, child := range children {
		columnIndex, funcs[i] = flattenRowFuncOf(columnIndex, child)
	}
	return funcs, columnIndex
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

	readRowValues func(*columnChunkReader, Row) (Row, error)
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

	if maxRepetitionLevel > 0 {
		ccr.readRowValues = (*columnChunkReader).readRowRepeatedValues
	} else {
		ccr.readRowValues = (*columnChunkReader).readRowValue
	}

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

func (ccr *columnChunkReader) readValue() (Value, error) {
	v, err := ccr.peekValue()
	ccr.nextValue()
	return v, err
}

func (ccr *columnChunkReader) peekValue() (Value, error) {
	if ccr.peeked {
		return ccr.cursor, nil
	}

	v, err := ccr.values.ReadValue()
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

func (ccr *columnChunkReader) readRowValue(row Row) (Row, error) {
	v, err := ccr.values.ReadValue()
	if err == nil {
		row = append(row, v)
	}
	return row, err
}

func (ccr *columnChunkReader) readRowRepeatedValues(row Row) (Row, error) {
	v, err := ccr.readValue()
	if err != nil {
		return row, err
	}
	row = append(row, v)
	for {
		v, err := ccr.peekValue()
		if err != nil {
			if err == io.EOF {
				break
			}
			return row, err
		}
		if v.repetitionLevel == 0 {
			break
		}
		row = append(row, v)
		ccr.nextValue()
	}
	return row, nil
}

func (ccr *columnChunkReader) readRow(row Row) (Row, error) {
readNextValue:
	if ccr.reader != nil {
		var err error
		if row, err = ccr.readRowValues(ccr, row); err != nil {
			if err == io.EOF {
				goto readNextPage
			}
		}
		return row, err
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
					return row, err
				}
				goto readNextPage
			case DataPageHeader:
				ccr.readDataPage(header)
				goto readNextValue
			default:
				return row, fmt.Errorf("unsupported page header type: %#v", header)
			}
		}
	}

	if !ccr.chunks.Next() {
		return row, io.EOF
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
		ccr.values.Reset(ccr.reader)
	}

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
