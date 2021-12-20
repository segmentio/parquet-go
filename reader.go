package parquet

import (
	"fmt"
	"io"
	"reflect"
)

type Reader struct {
	file    *File
	schema  *Schema
	seen    reflect.Type
	columns []*columnChunkReader
	buffers [][]Value
	indexes []int
	values  []Value
}

func NewReader(r io.ReaderAt, size int64, options ...ReaderOption) *Reader {
	f, err := OpenFile(r, size)
	if err != nil {
		panic(err)
	}
	return NewFileReader(f, options...)
}

func NewFileReader(file *File, options ...ReaderOption) *Reader {
	config := &ReaderConfig{
		PageBufferSize: DefaultPageBufferSize,
	}
	config.Apply(options...)
	if err := config.Validate(); err != nil {
		panic(err)
	}
	root := file.Root()
	columns := make([]*columnChunkReader, 0, numColumnsOf(root))
	root.forEachLeaf(func(column *Column) {
		columns = append(columns, newColumnChunkReader(column, config))
	})
	return &Reader{
		file:    file,
		schema:  NewSchema(root.Name(), root),
		columns: columns,
		buffers: make([][]Value, len(columns)),
		indexes: make([]int, len(columns)),
		values:  make([]Value, 0, len(columns)),
	}
}

func (r *Reader) ReadRow(row interface{}) error {
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
			clearRow(b)
			r.buffers[i] = b[:0]
		}
		clearRow(r.values)
		r.values = r.values[:0]
	}()

	for i, c := range r.columns {
		if r.buffers[i], err = c.readRow(r.buffers[i][:0]); err != nil {
			return err
		}
	}

	for i, b := range r.buffers {
		for j := range b {
			b[j].setColumnIndex(int8(i))
		}
	}

	for i := range r.indexes {
		r.indexes[i] = 0
	}

	r.values, _ = flattenRow(r.values, r.indexes, r.buffers, r.file.Root(), 0, 0)
	return r.schema.Reconstruct(row, r.values)
}

func clearRow(row []Value) {
	for i := range row {
		row[i] = Value{}
	}
}

func flattenRow(row Row, indexes []int, buffers [][]Value, col *Column, columnIndex, repetitionLevel int) (Row, int) {
	if len(col.columns) == 0 {
		repeated := col.Repeated()
		i := indexes[columnIndex]
		b := buffers[columnIndex]

		if i < len(b) {
			for {
				row = append(row, b[i])
				i++
				indexes[columnIndex]++
				if !repeated || i == len(b) || b[i].repetitionLevel < int8(repetitionLevel) {
					break
				}
			}
		}

		columnIndex++
	} else {
		startIndex := columnIndex
		repeated := col.Repeated()
		if repeated {
			repetitionLevel++
		}

		for {
			for _, child := range col.columns {
				row, columnIndex = flattenRow(row, indexes, buffers, child, columnIndex, repetitionLevel)
			}
			if !repeated {
				break
			}

			available, consumed := 0, 0
			for i := startIndex; i < columnIndex; i++ {
				available += len(buffers[i])
				consumed += indexes[i]
			}
			if consumed == available {
				break
			}

			columnIndex = startIndex
		}
	}
	return row, columnIndex
}

type columnChunkReader struct {
	bufferSize         int
	typ                Type
	maxRepetitionLevel int8
	maxDefinitionLevel int8

	chunks     *ColumnChunks
	pages      *ColumnPages
	reader     *DataPageReader
	dictionary Dictionary
	numPages   int

	peeked bool
	cursor Value

	readRowValues func(*columnChunkReader, Row) (Row, error)
}

func newColumnChunkReader(column *Column, config *ReaderConfig) *columnChunkReader {
	ccr := &columnChunkReader{
		bufferSize:         config.PageBufferSize,
		typ:                column.Type(),
		maxRepetitionLevel: column.MaxRepetitionLevel(),
		maxDefinitionLevel: column.MaxDefinitionLevel(),
		chunks:             column.Chunks(),
	}

	if column.MaxRepetitionLevel() > 0 {
		ccr.readRowValues = (*columnChunkReader).readRowRepeatedValues
	} else {
		ccr.readRowValues = (*columnChunkReader).readRowValue
	}

	if ccr.maxRepetitionLevel > 0 || ccr.maxDefinitionLevel > 0 {
		ccr.bufferSize /= 2
	}

	return ccr
}

func (ccr *columnChunkReader) Close() error {
	ccr.chunks.close(nil)
	ccr.pages = nil
	ccr.reader = nil
	ccr.dictionary = nil
	ccr.numPages = 0
	ccr.peeked = false
	ccr.cursor = Value{}
	return nil
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

	v, err := ccr.reader.ReadValue()
	if err != nil {
		return Value{}, err
	}

	ccr.peeked = true
	ccr.cursor = v.Clone() // TODO: optimize
	return ccr.cursor, nil
}

func (ccr *columnChunkReader) nextValue() {
	ccr.peeked = false
	ccr.cursor = Value{}
}

func (ccr *columnChunkReader) readRowValue(row Row) (Row, error) {
	v, err := ccr.reader.ReadValue()
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
				ccr.reader = nil
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
			switch header := ccr.pages.PageHeader().(type) {
			case DictionaryPageHeader:
				if ccr.numPages != 0 {
					return row, fmt.Errorf("the dictionary must be in the first page but one was found after reading %d pages", ccr.numPages)
				}

				ccr.dictionary = ccr.typ.NewDictionary(0)
				if err := ccr.dictionary.ReadFrom(
					header.Encoding().NewDecoder(ccr.pages.PageData()),
				); err != nil {
					return row, err
				}

				ccr.numPages++
				goto readNextPage

			case DataPageHeader:
				pageReader := (PageReader)(nil)
				pageData := header.Encoding().NewDecoder(ccr.pages.PageData())

				if ccr.dictionary != nil {
					pageReader = NewIndexedPageReader(pageData, ccr.bufferSize, ccr.dictionary)
				} else {
					pageReader = ccr.typ.NewPageReader(pageData, ccr.bufferSize)
				}

				ccr.reader = NewDataPageReader(
					header.RepetitionLevelEncoding().NewDecoder(ccr.pages.RepetitionLevels()),
					header.DefinitionLevelEncoding().NewDecoder(ccr.pages.DefinitionLevels()),
					header.NumValues(),
					pageReader,
					ccr.maxRepetitionLevel,
					ccr.maxDefinitionLevel,
					ccr.bufferSize,
				)

				ccr.numPages++
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
