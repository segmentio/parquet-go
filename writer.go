package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

// A Writer uses a parquet schema and sequence of Go values to produce a parquet
// file to an io.Writer.
//
// This example showcases a typical use of parquet writers:
//
//	writer := parquet.NewWriter(output)
//
//	for _, row := range rows {
//		if err := writer.Write(row); err != nil {
//			...
//		}
//	}
//
//	if err := writer.Close(); err != nil {
//		...
//	}
//
// The Writer type optimizes for minimal memory usage, each page is written as
// soon as it has been filled so only a single page per column needs to be held
// in memory and as a result, there are no opportunities to sort rows within an
// entire row group. Programs that need to produce parquet files with sorted
// row groups should use the Buffer type to buffer and sort the rows prior to
// writing them to a Writer.
type Writer struct {
	output io.Writer
	config *WriterConfig
	schema *Schema
	writer *writer
	values []Value
}

func NewWriter(output io.Writer, options ...WriterOption) *Writer {
	config := DefaultWriterConfig()
	config.Apply(options...)
	err := config.Validate()
	if err != nil {
		panic(err)
	}

	w := &Writer{
		output: output,
		config: config,
	}

	if config.Schema != nil {
		w.configure(config.Schema)
	}

	return w
}

func (w *Writer) configure(schema *Schema) {
	if schema != nil {
		w.config.Schema = schema
		w.schema = schema
		w.writer = newWriter(w.output, w.config)
	}
}

// Close must be called after all values were produced to the writer in order to
// flush all buffers and write the parquet footer.
func (w *Writer) Close() error {
	if w.writer != nil {
		return w.writer.Close()
	}
	return nil
}

// Reset clears the state of the writer without flushing any of the buffers,
// and setting the output to the io.Writer passed as argument, allowing the
// writer to be reused to produce another parquet file.
//
// Reset may be called at any time, including after a writer was closed.
func (w *Writer) Reset(output io.Writer) {
	if w.output = output; w.writer != nil {
		w.writer.Reset(w.output)
	}
}

// Write is called to write another row to the parquet file.
//
// The method uses the parquet schema configured on w to traverse the Go value
// and decompose it into a set of columns and values. If no schema were passed
// to NewWriter, it is deducted from the Go type of the row, which then have to
// be a struct or pointer to struct.
func (w *Writer) Write(row interface{}) error {
	if w.schema == nil {
		w.configure(SchemaOf(row))
	}
	defer func() {
		clearValues(w.values)
	}()
	w.values = w.schema.Deconstruct(w.values[:0], row)
	return w.WriteRow(w.values)
}

// WriteRow is called to write another row to the parquet file.
//
// The Writer must have been given a schema when NewWriter was called, otherwise
// the structure of the parquet file cannot be determined from the row only.
//
// The row is expected to contain values for each column of the writer's schema,
// in the order produced by the parquet.(*Schema).Deconstruct method.
func (w *Writer) WriteRow(row Row) error { return w.writer.WriteRow(row) }

// WriteRowGroup writes a row group to the parquet file.
//
// Buffered rows will be flushed prior to writing rows from the group, unless
// the row group was empty in which case nothing is written to the file.
// ReadRowsFrom reads rows from the reader passed as arguments and writes them
// to w.
//
// The content of the row group is flushed to the writer; after the method
// returns successfully, the row group will be empty and in ready to be reused.
func (w *Writer) WriteRowGroup(rowGroup RowGroup) (int64, error) {
	rowGroupSchema := rowGroup.Schema()
	switch {
	case rowGroupSchema == nil:
		return 0, ErrRowGroupSchemaMissing
	case w.schema == nil:
		w.configure(rowGroupSchema)
	case !nodesAreEqual(w.schema, rowGroupSchema):
		return 0, ErrRowGroupSchemaMismatch
	}
	return w.writer.WriteRowGroup(rowGroup)
}

// ReadRowsFrom reads rows from the reader passed as arguments and writes them
// to w.
//
// This is similar to calling WriteRow repeatedly, but will be more efficient
// if optimizations are supported by the reader.
func (w *Writer) ReadRowsFrom(rows RowReader) (written int64, err error) {
	if w.schema == nil {
		if r, ok := rows.(RowReaderWithSchema); ok {
			w.configure(r.Schema())
		}
	}
	written, w.values, err = copyRows(w.writer, rows, w.values[:0])
	return written, err
}

// Schema returns the schema of rows written by w.
//
// The returned value will be nil if no schema has yet been configured on w.
func (w *Writer) Schema() *Schema { return w.schema }

type writerRowGroup struct{ *writer }

func (g writerRowGroup) NumRows() int                    { return int(g.rowGroup.numRows) }
func (g writerRowGroup) NumColumns() int                 { return len(g.columns) }
func (g writerRowGroup) Column(i int) ColumnChunk        { return g.columns[i] }
func (g writerRowGroup) Schema() *Schema                 { return g.rowGroup.schema }
func (g writerRowGroup) SortingColumns() []SortingColumn { return nil }
func (g writerRowGroup) Rows() RowReader                 { return &rowGroupRowReader{rowGroup: g} }
func (g writerRowGroup) Pages() PageReader               { return &rowGroupPageReader{rowGroup: g} }

type writer struct {
	writer offsetTrackingWriter
	pages  unbufferedPageWriter

	createdBy string
	metadata  []format.KeyValue

	rowGroup struct {
		schema         *Schema
		numRows        int64
		totalRowCount  int64
		targetByteSize int64
	}

	buffers struct {
		header bytes.Buffer
		page   bytes.Buffer
	}

	columns       []*columnChunkWriter
	columnChunk   []format.ColumnChunk
	columnIndex   []format.ColumnIndex
	offsetIndex   []format.OffsetIndex
	encodingStats [][]format.PageEncodingStats

	columnOrders   []format.ColumnOrder
	schemaElements []format.SchemaElement
	rowGroups      []format.RowGroup
	columnIndexes  [][]format.ColumnIndex
	offsetIndexes  [][]format.OffsetIndex
}

func newWriter(output io.Writer, config *WriterConfig) *writer {
	w := new(writer)
	w.writer.Reset(output)
	w.pages.writer = &w.writer
	w.createdBy = config.CreatedBy
	w.metadata = make([]format.KeyValue, 0, len(config.KeyValueMetadata))
	for k, v := range config.KeyValueMetadata {
		w.metadata = append(w.metadata, format.KeyValue{Key: k, Value: v})
	}
	sortKeyValueMetadata(w.metadata)
	w.rowGroup.schema = config.Schema
	w.rowGroup.targetByteSize = config.RowGroupTargetSize

	w.rowGroup.schema.forEachNode(func(name string, node Node) {
		nodeType := node.Type()

		repetitionType := (*format.FieldRepetitionType)(nil)
		if node != w.rowGroup.schema { // the root has no repetition type
			repetitionType = fieldRepetitionTypeOf(node)
		}

		// For backward compatibility with older readers, the parquet specification
		// recommends to set the scale and precision on schema elements when the
		// column is of logical type decimal.
		logicalType := nodeType.LogicalType()
		scale, precision := (*int32)(nil), (*int32)(nil)
		if logicalType != nil && logicalType.Decimal != nil {
			scale = &logicalType.Decimal.Scale
			precision = &logicalType.Decimal.Precision
		}

		typeLength := (*int32)(nil)
		if n := int32(nodeType.Length()); n > 0 {
			typeLength = &n
		}

		w.schemaElements = append(w.schemaElements, format.SchemaElement{
			Type:           nodeType.PhyiscalType(),
			TypeLength:     typeLength,
			RepetitionType: repetitionType,
			Name:           name,
			NumChildren:    int32(node.NumChildren()),
			ConvertedType:  nodeType.ConvertedType(),
			Scale:          scale,
			Precision:      precision,
			LogicalType:    logicalType,
		})
	})

	dataPageType := format.DataPage
	if config.DataPageVersion == 2 {
		dataPageType = format.DataPageV2
	}

	forEachLeafColumnOf(w.rowGroup.schema, func(leaf leafColumn) {
		encoding, compression := encodingAndCompressionOf(leaf.node)
		dictionary := Dictionary(nil)
		columnType := leaf.node.Type()

		if isDictionaryEncoding(encoding) {
			dictionary = columnType.NewDictionary(0)
			columnType = dictionary.Type()
		}

		c := &columnChunkWriter{
			pages:              bufferedPageWriter{pool: config.ColumnPageBuffers},
			columnPath:         leaf.path,
			columnType:         columnType,
			columnIndexer:      columnType.NewColumnIndexer(config.ColumnIndexSizeLimit),
			compression:        compression,
			dictionary:         dictionary,
			dataPageType:       dataPageType,
			maxRepetitionLevel: leaf.maxRepetitionLevel,
			maxDefinitionLevel: leaf.maxDefinitionLevel,
			columnIndex:        leaf.columnIndex,
			bufferSize:         config.PageBufferSize,
			writePageStats:     config.DataPageStatistics,
			encodings:          make([]format.Encoding, 0, 3),
			// Data pages in version 2 can omit compression when dictionary
			// encoding is employed; only the dictionary page needs to be
			// compressed, the data pages are encoded with the hybrid
			// RLE/Bit-Pack encoding which doesn't benefit from an extra
			// compression layer.
			isCompressed: compression.CompressionCodec() != format.Uncompressed && (dataPageType != format.DataPageV2 || dictionary == nil),
		}

		// Those buffers are scratch space used to generate the page header and
		// content, they are shared by all column chunks because they are only
		// used during calls to writeDictionaryPage or writeDataPage, which are
		// not done concurrently.
		c.header.buffer, c.page.buffer = &w.buffers.header, &w.buffers.page

		if leaf.maxRepetitionLevel > 0 {
			c.insert = (*columnChunkWriter).insertRepeated
			c.commit = (*columnChunkWriter).commitRepeated
			c.values = make([]Value, 0, 10)
		} else {
			c.insert = (*columnChunkWriter).WriteRow
			c.commit = func(*columnChunkWriter) error { return nil }
		}

		if leaf.maxDefinitionLevel > 0 {
			c.levels.encoder = RLE.NewEncoder(nil)
			c.encodings = addEncoding(c.encodings, format.RLE)
		}

		if isDictionaryEncoding(encoding) {
			c.encodings = addEncoding(c.encodings, format.Plain)
		}

		c.encodings = addEncoding(c.encodings, encoding.Encoding())
		c.page.encoder = encoding.NewEncoder(nil)
		sortPageEncodings(c.encodings)

		w.columns = append(w.columns, c)
	})

	w.columnChunk = make([]format.ColumnChunk, len(w.columns))
	w.columnIndex = make([]format.ColumnIndex, len(w.columns))
	w.offsetIndex = make([]format.OffsetIndex, len(w.columns))
	w.columnOrders = make([]format.ColumnOrder, len(w.columns))

	for i, c := range w.columns {
		w.columnChunk[i] = format.ColumnChunk{
			MetaData: format.ColumnMetaData{
				Type:             format.Type(c.columnType.Kind()),
				Encoding:         c.encodings,
				PathInSchema:     c.columnPath,
				Codec:            c.compression.CompressionCodec(),
				KeyValueMetadata: nil, // TODO
			},
		}
	}

	for i, c := range w.columns {
		w.columnOrders[i] = *c.columnType.ColumnOrder()
	}

	return w
}

func (w *writer) Reset(writer io.Writer) {
	w.writer.Reset(writer)

	for _, c := range w.columns {
		c.reset()
	}

	for i := range w.rowGroups {
		w.rowGroups[i] = format.RowGroup{}
	}

	for i := range w.columnIndexes {
		w.columnIndexes[i] = nil
	}

	for i := range w.offsetIndexes {
		w.offsetIndexes[i] = nil
	}

	w.rowGroups = w.rowGroups[:0]
	w.columnIndexes = w.columnIndexes[:0]
	w.offsetIndexes = w.offsetIndexes[:0]
}

func (w *writer) Close() error {
	defer w.writer.Reset(nil)

	if err := w.writeFileHeader(); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return w.writeFileFooter()
}

func (w *writer) Flush() error {
	if w.rowGroup.numRows == 0 {
		return nil // nothing to flush
	}
	defer func() { w.rowGroup.numRows = 0 }()

	for _, c := range w.columns {
		if err := c.flush(); err != nil {
			return err
		}
	}

	_, err := w.writeRowGroup(writerRowGroup{w})
	return err
}

func (w *writer) WriteRow(row Row) error {
	defer func() {
		for _, c := range w.columns {
			clearValues(c.values)
			c.values = c.values[:0]
		}
	}()

	for i := range row {
		c := w.columns[row[i].Column()]
		if err := c.insert(c, row[i:i+1]); err != nil {
			return err
		}
	}

	for i := range w.columns {
		c := w.columns[i]
		if err := c.commit(c); err != nil {
			return err
		}
	}

	w.rowGroup.numRows++

	totalByteSize := int64(0)
	for i := range w.columns {
		totalByteSize += w.columns[i].pages.size
	}
	if totalByteSize >= w.rowGroup.targetByteSize {
		return w.Flush()
	}
	return nil
}

func (w *writer) WriteRowGroup(rows RowGroup) (int64, error) {
	if err := w.Flush(); err != nil {
		return 0, err
	}
	return w.writeRowGroup(rows)
}

func (w *writer) writeRowGroup(rows RowGroup) (int64, error) {
	if err := w.writeFileHeader(); err != nil {
		return 0, err
	}

	defer func() {
		w.rowGroup.totalRowCount = 0

		for _, c := range w.columns {
			c.reset()
		}

		for i := range w.columnChunk {
			c := &w.columnChunk[i]
			// Reset the fields of column chunks that change between row groups,
			// but keep the ones that remain unchanged.
			c.MetaData.NumValues = 0
			c.MetaData.TotalUncompressedSize = 0
			c.MetaData.TotalCompressedSize = 0
			c.MetaData.DataPageOffset = 0
			c.MetaData.DictionaryPageOffset = 0
			c.MetaData.Statistics = format.Statistics{}
			c.MetaData.EncodingStats = c.MetaData.EncodingStats[:0]
			c.MetaData.BloomFilterOffset = 0
		}

		for i := range w.columnIndex {
			w.columnIndex[i] = format.ColumnIndex{}
		}

		for i, offsetIndex := range w.offsetIndex {
			w.offsetIndex[i] = format.OffsetIndex{PageLocations: offsetIndex.PageLocations[:0]}
		}
	}()

	fileOffset := w.writer.offset

	if _, err := CopyPages(w, rows.Pages()); err != nil {
		return 0, err
	}

	for i, c := range w.columns {
		w.columnIndex[i] = format.ColumnIndex(c.columnIndexer.ColumnIndex())
	}

	totalByteSize := int64(0)
	totalCompressedSize := int64(0)

	for i := range w.columnChunk {
		c := &w.columnChunk[i].MetaData
		sortPageEncodingStats(c.EncodingStats)
		totalByteSize += int64(c.TotalUncompressedSize)
		totalCompressedSize += int64(c.TotalCompressedSize)
	}

	rowGroupSortingColumns := rows.SortingColumns()
	rowGroupSchema := rows.Schema()
	sortingColumns := make([]format.SortingColumn, 0, len(rowGroupSortingColumns))
	forEachLeafColumnOf(rowGroupSchema, func(leaf leafColumn) {
		if sortingIndex := searchSortingColumn(rowGroupSortingColumns, leaf.path); sortingIndex < len(sortingColumns) {
			sortingColumns[sortingIndex] = format.SortingColumn{
				ColumnIdx:  int32(leaf.columnIndex),
				Descending: rowGroupSortingColumns[sortingIndex].Descending(),
				NullsFirst: rowGroupSortingColumns[sortingIndex].NullsFirst(),
			}
		}
	})

	columns := make([]format.ColumnChunk, len(w.columnChunk))
	copy(columns, w.columnChunk)

	columnIndex := make([]format.ColumnIndex, len(w.columnIndex))
	copy(columnIndex, w.columnIndex)

	offsetIndex := make([]format.OffsetIndex, len(w.offsetIndex))
	copy(offsetIndex, w.offsetIndex)

	numRows := int64(rows.NumRows())
	w.rowGroups = append(w.rowGroups, format.RowGroup{
		Columns:             columns,
		TotalByteSize:       totalByteSize,
		NumRows:             numRows,
		SortingColumns:      sortingColumns,
		FileOffset:          fileOffset,
		TotalCompressedSize: totalCompressedSize,
		Ordinal:             int16(len(w.rowGroups)),
	})

	w.columnIndexes = append(w.columnIndexes, columnIndex)
	w.offsetIndexes = append(w.offsetIndexes, offsetIndex)
	return numRows, nil
}

func (w *writer) WritePage(page Page) (numValues int64, err error) {
	columnIndex := page.Column()
	columnChunk := &w.columnChunk[columnIndex].MetaData
	column := w.columns[columnIndex]

	if dictionary := page.Dictionary(); dictionary != nil && columnChunk.DictionaryPageOffset == 0 {
		columnChunk.DictionaryPageOffset = w.writer.offset

		if err := column.writeDictionaryPage(&w.pages, dictionary); err != nil {
			return 0, fmt.Errorf("writing dictionary page of row group colum %d: %w", columnIndex, err)
		}

		w.recordBufferedPageStats(columnIndex, columnChunk.DictionaryPageOffset)
	}

	if columnChunk.DataPageOffset == 0 {
		columnChunk.DataPageOffset = w.writer.offset
	}
	pageOffset := w.writer.offset

	switch p := page.(type) {
	case CompressedPage:
		numValues = int64(p.NumValues())
		compressedSize := int64(0)
		compressedSize, err = io.Copy(&w.writer, p.PageData())

		h := p.PageHeader()
		w.recordPageStats(columnIndex, pageOffset, pageStats{
			pageType:         h.PageType(),
			encoding:         h.Encoding(),
			uncompressedSize: int32(p.Size()),
			compressedSize:   int32(compressedSize),
			numRows:          int32(p.NumRows()),
		})

	case BufferedPage:
		numValues, err = column.writePage(&w.pages, p)
		w.recordBufferedPageStats(columnIndex, pageOffset)

	default:
		err = fmt.Errorf("cannot write page of type %T which is neither a parquet.CompressedPage nor parquet.BufferedPage", page)
	}

	columnChunk.NumValues += numValues
	return numValues, err
}

func (w *writer) recordBufferedPageStats(columnIndex int, pageOffset int64) {
	defer w.pages.reset()

	for _, stats := range w.pages.stats {
		w.recordPageStats(columnIndex, pageOffset, stats)
		pageOffset += int64(stats.compressedSize)
	}
}

func (w *writer) recordPageStats(columnIndex int, pageOffset int64, stats pageStats) {
	columnChunk := &w.columnChunk[columnIndex].MetaData
	offsetIndex := &w.offsetIndex[columnIndex]

	columnChunk.TotalUncompressedSize += int64(stats.uncompressedSize)
	columnChunk.TotalCompressedSize += int64(stats.compressedSize)

	columnChunk.EncodingStats = addPageEncodingStats(columnChunk.EncodingStats, format.PageEncodingStats{
		PageType: stats.pageType,
		Encoding: stats.encoding,
		Count:    1,
	})

	switch stats.pageType {
	case format.DataPage, format.DataPageV2:
		offsetIndex.PageLocations = append(offsetIndex.PageLocations, format.PageLocation{
			Offset:             pageOffset,
			CompressedPageSize: stats.compressedSize,
			FirstRowIndex:      w.rowGroup.totalRowCount,
		})
	}

	w.rowGroup.totalRowCount += int64(stats.numRows)
}

func (w *writer) writeFileHeader() error {
	if w.writer.writer == nil {
		return io.ErrClosedPipe
	}
	if w.writer.offset == 0 {
		_, err := w.writer.WriteString("PAR1")
		return err
	}
	return nil
}

func (w *writer) writeFileFooter() error {
	// The page index is composed of two sections: column and offset indexes.
	// They are written after the row groups, right before the footer (which
	// is written by the parent Writer.Close call).
	//
	// This section both writes the page index and generates the values of
	// ColumnIndexOffset, ColumnIndexLength, OffsetIndexOffset, and
	// OffsetIndexLength in the corresponding columns of the file metadata.
	//
	// Note: the page index is always written, even if we created data pages v1
	// because the parquet format is backward compatible in this case. Older
	// readers will simply ignore this section since they do not know how to
	// decode its content, nor have loaded any metadata to reference it.
	protocol := new(thrift.CompactProtocol)
	encoder := thrift.NewEncoder(protocol.NewWriter(&w.writer))

	for i, columnIndexes := range w.columnIndexes {
		rowGroup := &w.rowGroups[i]
		for j := range columnIndexes {
			column := &rowGroup.Columns[j]
			column.ColumnIndexOffset = w.writer.offset
			if err := encoder.Encode(&columnIndexes[j]); err != nil {
				return err
			}
			column.ColumnIndexLength = int32(w.writer.offset - column.ColumnIndexOffset)
		}
	}

	for i, offsetIndexes := range w.offsetIndexes {
		rowGroup := &w.rowGroups[i]
		for j := range offsetIndexes {
			column := &rowGroup.Columns[j]
			column.OffsetIndexOffset = w.writer.offset
			if err := encoder.Encode(&offsetIndexes[j]); err != nil {
				return err
			}
			column.OffsetIndexLength = int32(w.writer.offset - column.OffsetIndexOffset)
		}
	}

	numRows := int64(0)
	for rowGroupIndex := range w.rowGroups {
		numRows += w.rowGroups[rowGroupIndex].NumRows
	}

	footer, err := thrift.Marshal(new(thrift.CompactProtocol), &format.FileMetaData{
		Version:          1,
		Schema:           w.schemaElements,
		NumRows:          numRows,
		RowGroups:        w.rowGroups,
		KeyValueMetadata: w.metadata,
		CreatedBy:        w.createdBy,
		ColumnOrders:     w.columnOrders,
	})
	if err != nil {
		return err
	}

	length := len(footer)
	footer = append(footer, 0, 0, 0, 0)
	footer = append(footer, "PAR1"...)
	binary.LittleEndian.PutUint32(footer[length:], uint32(length))

	_, err = w.writer.Write(footer)
	return err
}

type columnChunkWriter struct {
	insert func(*columnChunkWriter, Row) error
	commit func(*columnChunkWriter) error
	values []Value

	pages         bufferedPageWriter
	columnPath    columnPath
	columnType    Type
	columnIndexer ColumnIndexer
	column        ColumnBuffer
	compression   compress.Codec
	dictionary    Dictionary

	dataPageType       format.PageType
	maxRepetitionLevel int8
	maxDefinitionLevel int8

	levels struct {
		encoder encoding.Encoder
		// In data pages v1, the repetition and definition levels are prefixed
		// with the 4 bytes length of the sections. While the parquet-format
		// documentation indicates that the length prefix is part of the hybrid
		// RLE/Bit-Pack encoding, this is the only condition where it is used
		// so we treat it as a special case rather than implementing it in the
		// encoding.
		//
		// Reference https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
		v1 lengthPrefixedWriter
	}

	header struct {
		buffer   *bytes.Buffer
		protocol thrift.CompactProtocol
		encoder  thrift.Encoder
	}

	page struct {
		buffer       *bytes.Buffer
		compressed   compress.Writer
		uncompressed offsetTrackingWriter
		encoder      encoding.Encoder
	}

	dict struct {
		encoder plain.Encoder
	}

	maxValues      int32
	numValues      int32
	columnIndex    int
	bufferSize     int
	writePageStats bool
	isCompressed   bool
	encodings      []format.Encoding
}

func (ccw *columnChunkWriter) reset() {
	if ccw.column != nil {
		ccw.column.Reset()
	}
	ccw.columnIndexer.Reset()
	ccw.numValues = 0
}

func (ccw *columnChunkWriter) statistics(nullCount int64) format.Statistics {
	min, max := ccw.columnIndexer.Bounds()
	minValue := min.Bytes()
	maxValue := max.Bytes()
	return format.Statistics{
		Min:       minValue, // deprecated
		Max:       maxValue, // deprecated
		NullCount: nullCount,
		MinValue:  minValue,
		MaxValue:  maxValue,
	}
}

func (ccw *columnChunkWriter) flush() error {
	if ccw.numValues == 0 {
		return nil
	}

	defer func() {
		ccw.column.Reset()
		ccw.numValues = 0
	}()

	_, err := ccw.writeDataPage(&ccw.pages, ccw.column.Page())
	return err
}

func (ccw *columnChunkWriter) insertRepeated(row Row) error {
	ccw.values = append(ccw.values, row...)
	return nil
}

func (ccw *columnChunkWriter) commitRepeated() error {
	return ccw.WriteRow(ccw.values)
}

func (ccw *columnChunkWriter) newColumnBuffer() ColumnBuffer {
	column := ccw.columnType.NewColumnBuffer(ccw.columnIndex, ccw.bufferSize)
	switch {
	case ccw.maxRepetitionLevel > 0:
		column = newRepeatedColumnBuffer(column, ccw.maxRepetitionLevel, ccw.maxDefinitionLevel, nullsGoLast)
	case ccw.maxDefinitionLevel > 0:
		column = newOptionalColumnBuffer(column, ccw.maxDefinitionLevel, nullsGoLast)
	}
	return column
}

func (ccw *columnChunkWriter) WriteRow(row Row) error {
	if ccw.column == nil {
		// Lazily create the row group column so we don't need to allocate it if
		// only WriteRowGroup is called on the parent row group writer.
		ccw.column = ccw.newColumnBuffer()
		ccw.maxValues = int32(ccw.column.Cap())
	}

	if ccw.numValues > 0 && (ccw.numValues+int32(len(row))) > ccw.maxValues {
		if err := ccw.flush(); err != nil {
			return err
		}
	}

	if err := ccw.column.WriteRow(row); err != nil {
		return err
	}
	ccw.numValues += int32(len(row))
	return nil
}

func (ccw *columnChunkWriter) writePage(writer pageWriter, page BufferedPage) (numValues int64, err error) {
	if err := ccw.flush(); err != nil {
		return 0, err
	}

	forEachPageSlice(page, int64(ccw.bufferSize), func(page BufferedPage) bool {
		var n int64
		n, err = ccw.writeDataPage(writer, page)
		numValues += n
		return err == nil
	})

	return numValues, err
}

func (ccw *columnChunkWriter) writeDataPage(writer pageWriter, page BufferedPage) (int64, error) {
	numValues := page.NumValues()
	if numValues == 0 {
		return 0, nil
	}

	ccw.page.buffer.Reset()
	repetitionLevelsByteLength := int32(0)
	definitionLevelsByteLength := int32(0)

	if ccw.dataPageType == format.DataPageV2 {
		if ccw.maxRepetitionLevel > 0 {
			ccw.page.uncompressed.Reset(ccw.page.buffer)
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxRepetitionLevel))
			ccw.levels.encoder.EncodeInt8(page.RepetitionLevels())
			repetitionLevelsByteLength = int32(ccw.page.uncompressed.offset)
		}
		if ccw.maxDefinitionLevel > 0 {
			ccw.page.uncompressed.Reset(ccw.page.buffer)
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxDefinitionLevel))
			ccw.levels.encoder.EncodeInt8(page.DefinitionLevels())
			definitionLevelsByteLength = int32(ccw.page.uncompressed.offset)
		}
	}

	if !ccw.isCompressed {
		ccw.page.uncompressed.Reset(ccw.page.buffer)
	} else {
		p, err := ccw.compressedPage(ccw.page.buffer)
		if err != nil {
			return 0, err
		}
		ccw.page.uncompressed.Reset(p)
	}

	if ccw.dataPageType == format.DataPage {
		if ccw.maxRepetitionLevel > 0 {
			ccw.levels.v1.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.Reset(&ccw.levels.v1)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxRepetitionLevel))
			ccw.levels.encoder.EncodeInt8(page.RepetitionLevels())
			ccw.levels.v1.Close()
		}
		if ccw.maxDefinitionLevel > 0 {
			ccw.levels.v1.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.Reset(&ccw.levels.v1)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxDefinitionLevel))
			ccw.levels.encoder.EncodeInt8(page.DefinitionLevels())
			ccw.levels.v1.Close()
		}
	}

	numNulls := page.NumNulls()
	minValue, maxValue := page.Bounds()
	statistics := ccw.makePageStatistics(int64(numNulls), minValue, maxValue)
	ccw.columnIndexer.IndexPage(numValues, numNulls, minValue, maxValue)

	ccw.page.encoder.Reset(&ccw.page.uncompressed)
	if err := page.WriteTo(ccw.page.encoder); err != nil {
		return 0, err
	}
	if ccw.page.compressed != nil {
		if err := ccw.page.compressed.Close(); err != nil {
			return 0, err
		}
	}

	ccw.header.buffer.Reset()
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(ccw.header.buffer))
	levelsByteLength := repetitionLevelsByteLength + definitionLevelsByteLength
	uncompressedPageSize := ccw.page.uncompressed.offset + int64(levelsByteLength)
	compressedPageSize := ccw.page.buffer.Len()
	encoding := ccw.page.encoder.Encoding()

	pageHeader := &format.PageHeader{
		Type:                 ccw.dataPageType,
		UncompressedPageSize: int32(uncompressedPageSize),
		CompressedPageSize:   int32(compressedPageSize),
		CRC:                  int32(crc32.ChecksumIEEE(ccw.page.buffer.Bytes())),
	}

	numRows := page.NumRows()
	switch ccw.dataPageType {
	case format.DataPage:
		pageHeader.DataPageHeader = &format.DataPageHeader{
			NumValues:               int32(numValues),
			Encoding:                encoding,
			DefinitionLevelEncoding: format.RLE,
			RepetitionLevelEncoding: format.RLE,
			Statistics:              statistics,
		}
	case format.DataPageV2:
		pageHeader.DataPageHeaderV2 = &format.DataPageHeaderV2{
			NumValues:                  int32(numValues),
			NumNulls:                   int32(numNulls),
			NumRows:                    int32(numRows),
			Encoding:                   encoding,
			DefinitionLevelsByteLength: definitionLevelsByteLength,
			RepetitionLevelsByteLength: repetitionLevelsByteLength,
			IsCompressed:               &ccw.isCompressed,
			Statistics:                 statistics,
		}
	}

	if err := ccw.header.encoder.Encode(pageHeader); err != nil {
		return 0, err
	}

	headerSize := ccw.header.buffer.Len()
	return int64(numValues), writer.writePage(ccw.header.buffer, ccw.page.buffer, pageStats{
		pageType:         ccw.dataPageType,
		encoding:         encoding,
		numNulls:         int32(numNulls),
		numValues:        int32(numValues),
		numRows:          int32(numRows),
		uncompressedSize: int32(headerSize) + int32(uncompressedPageSize),
		compressedSize:   int32(headerSize) + int32(compressedPageSize),
	})
}

func (ccw *columnChunkWriter) compressedPage(w io.Writer) (compress.Writer, error) {
	if ccw.page.compressed == nil {
		z, err := ccw.compression.NewWriter(w)
		if err != nil {
			return nil, fmt.Errorf("creating compressor for parquet column chunk writer: %w", err)
		}
		ccw.page.compressed = z
	} else {
		if err := ccw.page.compressed.Reset(w); err != nil {
			return nil, fmt.Errorf("resetting compressor for parquet column chunk writer: %w", err)
		}
	}
	return ccw.page.compressed, nil
}

func (ccw *columnChunkWriter) writeDictionaryPage(writer pageWriter, dict Dictionary) error {
	ccw.page.buffer.Reset()

	p, err := ccw.compressedPage(ccw.page.buffer)
	if err != nil {
		return err
	}

	ccw.page.uncompressed.Reset(p)
	ccw.dict.encoder.Reset(&ccw.page.uncompressed)

	if err := dict.WriteTo(&ccw.dict.encoder); err != nil {
		return fmt.Errorf("writing parquet dictionary page: %w", err)
	}
	if err := p.Close(); err != nil {
		return fmt.Errorf("flushing compressed parquet dictionary page: %w", err)
	}

	ccw.header.buffer.Reset()
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(ccw.header.buffer))

	if err := ccw.header.encoder.Encode(&format.PageHeader{
		Type:                 format.DictionaryPage,
		UncompressedPageSize: int32(ccw.page.uncompressed.offset),
		CompressedPageSize:   int32(ccw.page.buffer.Len()),
		CRC:                  int32(crc32.ChecksumIEEE(ccw.page.buffer.Bytes())),
		DictionaryPageHeader: &format.DictionaryPageHeader{
			NumValues: int32(dict.Len()),
			Encoding:  format.Plain,
			IsSorted:  false,
		},
	}); err != nil {
		return err
	}

	headerSize := ccw.header.buffer.Len()
	return writer.writePage(ccw.header.buffer, ccw.page.buffer, pageStats{
		pageType:         format.DictionaryPage,
		encoding:         format.Plain,
		uncompressedSize: int32(headerSize) + int32(ccw.page.uncompressed.offset),
		compressedSize:   int32(headerSize) + int32(ccw.page.buffer.Len()),
	})
}

func (ccw *columnChunkWriter) makePageStatistics(numNulls int64, minValue, maxValue Value) (stats format.Statistics) {
	if ccw.writePageStats {
		minValueBytes := minValue.Bytes()
		maxValueBytes := maxValue.Bytes()
		stats = format.Statistics{
			Min:       minValueBytes, // deprecated
			Max:       maxValueBytes, // deprecated
			NullCount: numNulls,
			MinValue:  minValueBytes,
			MaxValue:  maxValueBytes,
		}
	}
	return stats
}

func (ccw *columnChunkWriter) Column() int {
	return ccw.columnIndex
}

func (ccw *columnChunkWriter) Pages() PageReader {
	return ccw.pages.read(ccw)
}

func addEncoding(encodings []format.Encoding, add format.Encoding) []format.Encoding {
	for _, enc := range encodings {
		if enc == add {
			return encodings
		}
	}
	return append(encodings, add)
}

func addPageEncodingStats(stats []format.PageEncodingStats, pages ...format.PageEncodingStats) []format.PageEncodingStats {
addPages:
	for _, add := range pages {
		for i, st := range stats {
			if st.PageType == add.PageType && st.Encoding == add.Encoding {
				stats[i].Count += add.Count
				continue addPages
			}
		}
		stats = append(stats, add)
	}
	return stats
}

func sortPageEncodings(encodings []format.Encoding) {
	sort.Slice(encodings, func(i, j int) bool {
		return encodings[i] < encodings[j]
	})
}

func sortPageEncodingStats(stats []format.PageEncodingStats) {
	sort.Slice(stats, func(i, j int) bool {
		s1 := &stats[i]
		s2 := &stats[j]
		if s1.PageType != s2.PageType {
			return s1.PageType < s2.PageType
		}
		return s1.Encoding < s2.Encoding
	})
}

type lengthPrefixedWriter struct {
	writer io.Writer
	buffer []byte
}

func (w *lengthPrefixedWriter) Reset(ww io.Writer) {
	w.writer = ww
	w.buffer = append(w.buffer[:0], 0, 0, 0, 0)
}

func (w *lengthPrefixedWriter) Close() error {
	if len(w.buffer) > 0 {
		defer func() { w.buffer = w.buffer[:0] }()
		binary.LittleEndian.PutUint32(w.buffer, uint32(len(w.buffer))-4)
		_, err := w.writer.Write(w.buffer)
		return err
	}
	return nil
}

func (w *lengthPrefixedWriter) Write(b []byte) (int, error) {
	w.buffer = append(w.buffer, b...)
	return len(b), nil
}

type offsetTrackingWriter struct {
	writer io.Writer
	offset int64
}

func (w *offsetTrackingWriter) Reset(writer io.Writer) {
	w.writer = writer
	w.offset = 0
}

func (w *offsetTrackingWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.offset += int64(n)
	return n, err
}

func (w *offsetTrackingWriter) WriteString(s string) (int, error) {
	n, err := io.WriteString(w.writer, s)
	w.offset += int64(n)
	return n, err
}

type pageStats struct {
	pageType         format.PageType
	encoding         format.Encoding
	numNulls         int32
	numValues        int32
	numRows          int32
	uncompressedSize int32
	compressedSize   int32
}

type pageWriter interface {
	writePage(header, data io.Reader, stats pageStats) error
}

type bufferedPageWriter struct {
	pool  PageBufferPool
	size  int64
	pages []compressedPage
}

func (w *bufferedPageWriter) read(column *columnChunkWriter) *bufferedPageReader {
	for i := range w.pages {
		w.pages[i].column = column
	}
	return &bufferedPageReader{pages: w.pages}
}

func (w *bufferedPageWriter) reset() {
	for _, page := range w.pages {
		w.pool.PutPageBuffer(page.buffer)
	}
	for i := range w.pages {
		w.pages[i] = compressedPage{}
	}
	w.size = 0
	w.pages = w.pages[:0]
}

func (w *bufferedPageWriter) writePage(header, data io.Reader, stats pageStats) error {
	buffer := w.pool.GetPageBuffer()
	defer func() {
		if buffer != nil {
			w.pool.PutPageBuffer(buffer)
		}
	}()
	if _, err := io.Copy(buffer, header); err != nil {
		return err
	}
	if _, err := io.Copy(buffer, data); err != nil {
		return err
	}
	w.pages = append(w.pages, compressedPage{
		buffer: buffer,
		stats:  stats,
	})
	w.size += int64(stats.uncompressedSize)
	buffer = nil
	return nil
}

type bufferedPageReader struct {
	pages []compressedPage
}

func (r *bufferedPageReader) ReadPage() (Page, error) {
	if len(r.pages) == 0 {
		return nil, io.EOF
	}
	page := &r.pages[0]
	r.pages = r.pages[1:]
	return page, nil
}

type compressedPage struct {
	column *columnChunkWriter
	buffer io.ReadWriter
	stats  pageStats
}

func (page *compressedPage) Column() int              { return page.column.columnIndex }
func (page *compressedPage) Dictionary() Dictionary   { return page.column.dictionary }
func (page *compressedPage) NumRows() int             { return int(page.stats.numRows) }
func (page *compressedPage) NumValues() int           { return int(page.stats.numValues) }
func (page *compressedPage) NumNulls() int            { return int(page.stats.numNulls) }
func (page *compressedPage) Bounds() (min, max Value) { return }
func (page *compressedPage) Slice(i, j int) Page      { return nil }
func (page *compressedPage) Size() int64              { return int64(page.stats.uncompressedSize) }
func (page *compressedPage) Values() ValueReader      { return nil }
func (page *compressedPage) PageHeader() PageHeader   { return compressedPageHeader{page} }
func (page *compressedPage) PageData() io.Reader      { return page.buffer }

type compressedPageHeader struct{ *compressedPage }

func (header compressedPageHeader) NumValues() int            { return int(header.stats.numValues) }
func (header compressedPageHeader) Encoding() format.Encoding { return header.stats.encoding }
func (header compressedPageHeader) PageType() format.PageType { return header.stats.pageType }

type unbufferedPageWriter struct {
	writer io.Writer
	stats  []pageStats
}

func (w *unbufferedPageWriter) reset() {
	w.stats = w.stats[:0]
}

func (w *unbufferedPageWriter) writePage(header, data io.Reader, stats pageStats) error {
	if _, err := io.Copy(w.writer, header); err != nil {
		return err
	}
	if _, err := io.Copy(w.writer, data); err != nil {
		return err
	}
	w.stats = append(w.stats, stats)
	return nil
}

var (
	_ RowWriterWithSchema = (*Writer)(nil)
	_ RowReaderFrom       = (*Writer)(nil)
	_ RowGroupWriter      = (*Writer)(nil)

	_ RowWriter      = (*writer)(nil)
	_ RowGroupWriter = (*writer)(nil)
	_ PageWriter     = (*writer)(nil)

	_ ColumnChunk = (*columnChunkWriter)(nil)
	_ RowWriter   = (*columnChunkWriter)(nil)

	_ CompressedPage = (*compressedPage)(nil)
)
