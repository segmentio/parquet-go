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
	file       *File
	fileSchema *Schema
	readSchema *Schema
	seen       reflect.Type
	columns    []*columnChunkReader
	readers    []columnValueReader
	buffer     []Value
	values     []Value
	conv       Conversion
	read       columnReadRowFunc
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
	readers := makeColumnValueReaders(len(columns), func(i int) ValueReader {
		return columns[i]
	})
	_, read := columnReadRowFuncOf(root, 0, 0)
	schema := NewSchema(root.Name(), root)
	return &Reader{
		file:       f,
		fileSchema: schema,
		readSchema: schema,
		columns:    columns,
		readers:    readers,
		values:     make([]Value, 0, len(columns)),
		read:       read,
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
	for i, c := range r.columns {
		c.Reset()
		r.readers[i].reset(c)
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
	buf, err := r.read(buf, 0, r.readers)
	if err == nil && len(buf) == n {
		err = io.EOF
	}
	return buf, err
}

// Schema returns the schema of rows read by r.
func (r *Reader) Schema() *Schema { return r.fileSchema }

type columnValueReader struct {
	buffer []Value
	offset uint
	reader ValueReader
}

func makeColumnValueReaders(numColumns int, columnAt func(int) ValueReader) []columnValueReader {
	const columnBufferSize = defaultValueBufferSize
	buffer := make([]Value, columnBufferSize*numColumns)
	readers := make([]columnValueReader, numColumns)

	for i := 0; i < numColumns; i++ {
		readers[i].buffer = buffer[:0:columnBufferSize]
		readers[i].reader = columnAt(i)
		buffer = buffer[columnBufferSize:]
	}

	return readers
}

func (r *columnValueReader) reset(reader ValueReader) {
	r.buffer = r.buffer[:0]
	r.offset = 0
	r.reader = reader
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
func columnReadRowFuncOfLeaf(node Node, columnIndex int, repetitionDepth int8) (int, columnReadRowFunc) {
	var read columnReadRowFunc

	if repetitionDepth == 0 {
		read = func(row Row, _ int8, columns []columnValueReader) (Row, error) {
			col := &columns[columnIndex]

			for {
				if col.offset < uint(len(col.buffer)) {
					row = append(row, col.buffer[col.offset])
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
					if col.buffer[col.offset].repetitionLevel == repetitionLevel {
						row = append(row, col.buffer[col.offset])
						col.offset++
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

type columnChunkReader struct {
	bufferSize int
	column     *Column
	chunks     *ColumnChunks
	pages      *ColumnPages
	reader     *DataPageReader
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
		values:     make([]Value, 0, defaultValueBufferSize),
	}

	maxRepetitionLevel := column.MaxRepetitionLevel()
	maxDefinitionLevel := column.MaxDefinitionLevel()

	if maxRepetitionLevel > 0 || maxDefinitionLevel > 0 {
		ccr.bufferSize /= 2
	}

	ccr.page.typ = column.Type()
	return ccr
}

func (ccr *columnChunkReader) Reset() {
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

func (ccr *columnChunkReader) ReadValues(values []Value) (int, error) {
readNextValues:
	if ccr.reader != nil {
		n, err := ccr.reader.ReadValues(values)
		if n > 0 {
			return n, nil
		}
		if err != io.EOF {
			return 0, err
		}
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
					return 0, err
				}
				goto readNextPage
			case DataPageHeader:
				ccr.readDataPage(header)
				goto readNextValues
			default:
				return 0, fmt.Errorf("unsupported page header type: %#v", header)
			}
		}
	}

	if !ccr.chunks.Next() {
		return 0, io.EOF
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
		ccr.reader = NewDataPageReader(
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
	_ RowReaderWithSchema = (*Reader)(nil)
)
