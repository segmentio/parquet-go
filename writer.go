package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
// row groups should use the RowGroup type to buffer and sort the rows prior to
// writing them to a Writer.
type Writer struct {
	writer    io.Writer
	config    *WriterConfig
	schema    *Schema
	rowGroups *rowGroupWriter
	metadata  []format.KeyValue
	values    []Value
}

func NewWriter(writer io.Writer, options ...WriterOption) *Writer {
	config := DefaultWriterConfig()
	config.Apply(options...)
	err := config.Validate()
	if err != nil {
		panic(err)
	}

	w := &Writer{
		writer:   writer,
		config:   config,
		metadata: make([]format.KeyValue, 0, len(config.KeyValueMetadata)),
		values:   make([]Value, 0, MaxColumnIndex),
	}

	if config.Schema != nil {
		w.configure(config.Schema)
	}

	for k, v := range config.KeyValueMetadata {
		w.metadata = append(w.metadata, format.KeyValue{Key: k, Value: v})
	}

	sortKeyValueMetadata(w.metadata)
	return w
}

func (w *Writer) configure(schema *Schema) {
	w.schema = schema
	w.rowGroups = newRowGroupWriter(w.writer, w.schema, w.config)
}

// Close must be called after all values were produced to the writer in order to
// flush all buffers and write the parquet footer.
func (w *Writer) Close() error {
	if w.rowGroups != nil {
		return w.rowGroups.close(w.config.CreatedBy, w.metadata)
	}
	return nil
}

// Reset clears the state of the writer without flushing any of the buffers,
// and setting the output to the io.Writer passed as argument, allowing the
// writer to be reused to produce another parquet file.
//
// Reset may be called at any time, including after a writer was closed.
func (w *Writer) Reset(writer io.Writer) {
	if w.writer = writer; w.rowGroups != nil {
		w.rowGroups.reset(w.writer)
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
		for i := range w.values {
			w.values[i] = Value{}
		}
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
func (w *Writer) WriteRow(row Row) error { return w.rowGroups.writeRow(row) }

// WriteRowGroup writes a row group to the parquet file.
//
// The writer must have had no schema configured, or its schema must be the same
// as the schema of the row group or an error will be returned.
//
// Buffered rows will be flushed prior to writing rows from the group, unless
// the row group was empty in which case nothing is written to the file.
//
// The content of the row group is flushed to the writer; after the method
// returns successfully, the row group will be empty and in ready to be reused.
func (w *Writer) WriteRowGroup(rowGroup *RowGroup) error {
	if w.schema == nil {
		w.configure(rowGroup.schema)
	} else if w.schema != rowGroup.schema {
		return fmt.Errorf("cannot write row group with mismatching schema:\n%s\n%s", w.schema, rowGroup.schema)
	}
	return w.rowGroups.writeRowGroup(rowGroup)
}

type rowGroupWriter struct {
	writer countWriter
	pages  unbufferedPageWriter

	columns       []rowGroupWriterColumn
	colOrders     []format.ColumnOrder
	colSchema     []format.SchemaElement
	rowGroups     []format.RowGroup
	columnIndexes [][]format.ColumnIndex
	offsetIndexes [][]format.OffsetIndex

	numRows    int64
	fileOffset int64
	targetSize int64
}

type rowGroupWriterColumn struct {
	typ        format.Type
	codec      format.CompressionCodec
	path       []string
	dictionary Dictionary
	writer     *bufferedPageWriter
	chunks     *columnChunkWriter
}

func (col *rowGroupWriterColumn) reset() {
	if col.dictionary != nil {
		col.dictionary.Reset()
	}
	col.writer.reset()
	col.chunks.reset()
}

func newRowGroupWriter(writer io.Writer, schema *Schema, config *WriterConfig) *rowGroupWriter {
	rgw := &rowGroupWriter{
		writer: countWriter{writer: writer},
		// Assume this is the first row group in the file, it starts after the
		// "PAR1" magic number.
		fileOffset: 4,
		targetSize: config.RowGroupTargetSize,
	}

	dataPageType := format.DataPage
	if config.DataPageVersion == 2 {
		dataPageType = format.DataPageV2
	}

	rgw.init(schema, []string{schema.Name()}, dataPageType, 0, 0, config)
	rgw.pages.writer = &rgw.writer
	return rgw
}

func (rgw *rowGroupWriter) init(node Node, path []string, dataPageType format.PageType, maxRepetitionLevel, maxDefinitionLevel int8, config *WriterConfig) {
	columnType := node.Type()

	if !node.Required() {
		maxDefinitionLevel++
		if maxDefinitionLevel < 0 { // overflow
			panic("cannot represent parquet schema with more than 127 definition levels")
		}
	}
	if node.Repeated() {
		maxRepetitionLevel++
		if maxRepetitionLevel < 0 { // overflow
			panic("cannot represent parquet schema with more than 127 repetition levels")
		}
	}

	repetitionType := (*format.FieldRepetitionType)(nil)
	if len(path) > 1 { // the root has no repetition type
		repetitionType = fieldRepetitionTypeOf(node)
	}

	// For backward compatibility with older readers, the parquet specification
	// recommends to set the scale and precision on schema elements when the
	// column is of logical type decimal.
	logicalType := columnType.LogicalType()
	scale, precision := (*int32)(nil), (*int32)(nil)
	if logicalType != nil && logicalType.Decimal != nil {
		scale = &logicalType.Decimal.Scale
		precision = &logicalType.Decimal.Precision
	}

	typeLength := (*int32)(nil)
	if n := int32(columnType.Length()); n > 0 {
		typeLength = &n
	}

	rgw.colSchema = append(rgw.colSchema, format.SchemaElement{
		Type:           columnType.PhyiscalType(),
		TypeLength:     typeLength,
		RepetitionType: repetitionType,
		Name:           path[len(path)-1],
		NumChildren:    int32(node.NumChildren()),
		ConvertedType:  columnType.ConvertedType(),
		Scale:          scale,
		Precision:      precision,
		LogicalType:    logicalType,
	})

	if names := node.ChildNames(); len(names) > 0 {
		base := path[:len(path):len(path)]

		for _, name := range names {
			rgw.init(node.ChildByName(name), append(base, name), dataPageType, maxRepetitionLevel, maxDefinitionLevel, config)
		}
	} else {
		encoding, compression := encodingAndCompressionOf(node)
		dictionary := Dictionary(nil)

		if isDictionaryEncoding(encoding) {
			dictionary = columnType.NewDictionary(0)
			columnType = dictionary.Type()
		}

		buffer := &bufferedPageWriter{
			pool: config.ColumnPageBuffers,
		}

		rgw.columns = append(rgw.columns, rowGroupWriterColumn{
			typ:        format.Type(columnType.Kind()),
			codec:      compression.CompressionCodec(),
			path:       path[1:],
			dictionary: dictionary,
			writer:     buffer,
			chunks: newColumnChunkWriter(
				buffer,
				columnType,
				columnType.NewColumnIndexer(config.ColumnIndexSizeLimit),
				config.PageBufferSize,
				compression,
				encoding.NewEncoder(nil),
				dataPageType,
				maxRepetitionLevel,
				maxDefinitionLevel,
				config.DataPageStatistics,
				// Data pages in version 2 can omit compression when dictionary
				// encoding is employed; only the dictionary page needs to be
				// compressed, the data pages are encoded with the hybrid
				// RLE/Bit-Pack encoding which doesn't benefit from an extra
				// compression layer.
				compression.CompressionCodec() != format.Uncompressed && (dataPageType != format.DataPageV2 || dictionary == nil),
			),
		})

		rgw.colOrders = append(rgw.colOrders, *columnType.ColumnOrder())
	}
}

func (rgw *rowGroupWriter) reset(w io.Writer) {
	for i := range rgw.columns {
		rgw.columns[i].reset()
	}
	for i := range rgw.rowGroups {
		rgw.rowGroups[i] = format.RowGroup{}
	}
	for i := range rgw.columnIndexes {
		rgw.columnIndexes[i] = nil
	}
	for i := range rgw.offsetIndexes {
		rgw.offsetIndexes[i] = nil
	}
	rgw.writer.Reset(w)
	rgw.pages.reset()
	rgw.rowGroups = rgw.rowGroups[:0]
	rgw.columnIndexes = rgw.columnIndexes[:0]
	rgw.offsetIndexes = rgw.offsetIndexes[:9]
	rgw.numRows = 0
	rgw.fileOffset = 4
}

func (rgw *rowGroupWriter) close(createdBy string, metadata []format.KeyValue) error {
	if rgw.writer.writer == nil {
		return nil // already closed
	}
	defer func() {
		rgw.writer.writer = nil
	}()
	if err := rgw.flush(); err != nil {
		return err
	}

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
	encoder := thrift.NewEncoder(protocol.NewWriter(&rgw.writer))

	for i, columnIndexes := range rgw.columnIndexes {
		rowGroup := &rgw.rowGroups[i]
		for j := range columnIndexes {
			column := &rowGroup.Columns[j]
			column.ColumnIndexOffset = rgw.writer.length
			if err := encoder.Encode(&columnIndexes[j]); err != nil {
				return err
			}
			column.ColumnIndexLength = int32(rgw.writer.length - column.ColumnIndexOffset)
		}
	}

	for i, offsetIndexes := range rgw.offsetIndexes {
		rowGroup := &rgw.rowGroups[i]
		for j := range offsetIndexes {
			column := &rowGroup.Columns[j]
			column.OffsetIndexOffset = rgw.writer.length
			if err := encoder.Encode(&offsetIndexes[j]); err != nil {
				return err
			}
			column.OffsetIndexLength = int32(rgw.writer.length - column.OffsetIndexOffset)
		}
	}

	numRows := int64(0)
	for rowGroupIndex := range rgw.rowGroups {
		numRows += rgw.rowGroups[rowGroupIndex].NumRows
	}

	footer, err := thrift.Marshal(new(thrift.CompactProtocol), &format.FileMetaData{
		Version:          1,
		Schema:           rgw.colSchema,
		NumRows:          numRows,
		RowGroups:        rgw.rowGroups,
		KeyValueMetadata: metadata,
		CreatedBy:        createdBy,
		ColumnOrders:     rgw.colOrders,
	})
	if err != nil {
		return err
	}

	length := len(footer)
	footer = append(footer, 0, 0, 0, 0)
	footer = append(footer, "PAR1"...)
	binary.LittleEndian.PutUint32(footer[length:], uint32(length))

	_, err = rgw.writer.Write(footer)
	return err
}

func (rgw *rowGroupWriter) flush() error {
	if rgw.writer.writer == nil {
		return io.ErrClosedPipe
	}
	if rgw.numRows == 0 {
		return nil // nothing to flush
	}
	if rgw.writer.length == 0 {
		if _, err := rgw.writer.WriteString("PAR1"); err != nil {
			return err
		}
	}

	defer func() {
		for i := range rgw.columns {
			rgw.columns[i].reset()
		}
		rgw.pages.reset()
		rgw.numRows = 0
	}()

	for i := range rgw.columns {
		if err := rgw.columns[i].chunks.flush(); err != nil {
			return err
		}
	}

	totalRowCount := int64(0)
	totalByteSize := int64(0)
	totalCompressedSize := int64(0)
	columns := make([]format.ColumnChunk, len(rgw.columns))
	columnIndex := make([]format.ColumnIndex, len(rgw.columns))
	offsetIndex := make([]format.OffsetIndex, len(rgw.columns))

	for i := range rgw.columns {
		c := &rgw.columns[i]

		columnChunk := struct {
			dictionaryPageOffset  int64
			dataPageOffset        int64
			totalUncompressedSize int64
			totalCompressedSize   int64
			encodingStats         []format.PageEncodingStats
		}{
			encodingStats: make([]format.PageEncodingStats, 0, 3),
		}

		if c.dictionary != nil {
			columnChunk.dictionaryPageOffset = rgw.writer.length
			rgw.pages.reset()

			if err := c.chunks.writeDictionaryPage(&rgw.pages, c.dictionary); err != nil {
				return err
			}

			stats := rgw.pages.stats()
			columnChunk.totalCompressedSize += stats.totalCompressedSize
			columnChunk.totalUncompressedSize += stats.totalUncompressedSize
			columnChunk.encodingStats = append(columnChunk.encodingStats, stats.encodingStats...)
		}

		pages := c.writer.pages()
		columnChunk.dataPageOffset = rgw.writer.length
		columnOffsetIndex := &offsetIndex[i]
		columnOffsetIndex.PageLocations = make([]format.PageLocation, len(pages))
		copy(columnOffsetIndex.PageLocations, pages)

		for pageIndex, buffer := range c.writer.buffers {
			columnOffsetIndex.PageLocations[pageIndex].Offset = rgw.writer.length
			n, err := io.Copy(&rgw.writer, buffer)
			if err != nil {
				return err
			}
			compressedPageSize := int64(columnOffsetIndex.PageLocations[pageIndex].CompressedPageSize)
			if n != compressedPageSize {
				return fmt.Errorf("writing page %d of column %d: compressed page size is %d but %d bytes were written", pageIndex, i, compressedPageSize, n)
			}
		}

		stats := c.writer.stats()
		columnIndex[i] = format.ColumnIndex(c.chunks.columnIndexer.ColumnIndex())
		columnChunk.totalCompressedSize += stats.totalCompressedSize
		columnChunk.totalUncompressedSize += stats.totalUncompressedSize
		columnChunk.encodingStats = append(columnChunk.encodingStats, stats.encodingStats...)
		sortPageEncodingStats(columnChunk.encodingStats)

		columns[i] = format.ColumnChunk{
			MetaData: format.ColumnMetaData{
				Type:                  c.typ,
				Encoding:              c.chunks.encodings,
				PathInSchema:          c.path,
				Codec:                 c.codec,
				NumValues:             stats.numValues,
				TotalUncompressedSize: columnChunk.totalUncompressedSize,
				TotalCompressedSize:   columnChunk.totalCompressedSize,
				KeyValueMetadata:      nil,
				DataPageOffset:        columnChunk.dataPageOffset,
				DictionaryPageOffset:  columnChunk.dictionaryPageOffset,
				Statistics:            c.chunks.statistics(stats.nullCount),
				EncodingStats:         columnChunk.encodingStats,
				BloomFilterOffset:     0,
			},
		}

		totalRowCount += stats.totalRowCount
		totalByteSize += columnChunk.totalUncompressedSize
		totalCompressedSize += columnChunk.totalCompressedSize
	}

	rgw.rowGroups = append(rgw.rowGroups, format.RowGroup{
		Columns:             columns,
		TotalByteSize:       totalByteSize,
		NumRows:             totalRowCount,
		SortingColumns:      nil, // TODO
		FileOffset:          rgw.fileOffset,
		TotalCompressedSize: totalCompressedSize,
		Ordinal:             int16(len(rgw.rowGroups)),
	})

	rgw.columnIndexes = append(rgw.columnIndexes, columnIndex)
	rgw.offsetIndexes = append(rgw.offsetIndexes, offsetIndex)
	rgw.fileOffset += totalCompressedSize
	return nil
}

func (rgw *rowGroupWriter) writeRow(row Row) (err error) {
	if len(row) == 0 {
		panic("BUG: cannot write a row with no values")
	}

	defer func() {
		for i := range rgw.columns {
			rgw.columns[i].chunks.clear()
		}
	}()

	for i := range row {
		c := &rgw.columns[row[i].ColumnIndex()]
		w := c.chunks
		if err := w.insert(w, row[i:i+1]); err != nil {
			return err
		}
	}

	for i := range rgw.columns {
		w := rgw.columns[i].chunks
		if err := w.commit(w); err != nil {
			return err
		}
	}

	rgw.numRows++

	rowGroupSize := int64(0)
	for i := range rgw.columns {
		rowGroupSize += rgw.columns[i].writer.stats().totalCompressedSize
	}
	if rowGroupSize >= rgw.targetSize {
		return rgw.flush()
	}

	return nil
}

func (rgw *rowGroupWriter) writeRowGroup(rowGroup *RowGroup) error {
	if rowGroup.Len() == 0 {
		return nil
	}

	if err := rgw.flush(); err != nil {
		return err
	}
	defer rowGroup.Reset()

	for i, col := range rowGroup.columns {
		if dictionary := col.Dictionary(); dictionary != nil {
			if err := rgw.columns[i].chunks.writeDictionaryPage(&rgw.pages, dictionary); err != nil {
				return err
			}
		}
		if err := rgw.columns[i].chunks.writePage(&rgw.pages, col.Page()); err != nil {
			return err
		}
	}

	// TODO
	return nil
}

type columnChunkWriter struct {
	insert func(*columnChunkWriter, []Value) error
	commit func(*columnChunkWriter) error
	values []Value

	writer           pageWriter
	columnType       Type
	columnIndexer    ColumnIndexer
	columnBufferSize int
	column           RowGroupColumn
	compression      compress.Codec

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
		buffer   bytes.Buffer
		protocol thrift.CompactProtocol
		encoder  thrift.Encoder
	}

	page struct {
		buffer       bytes.Buffer
		checksum     crc32Writer
		compressed   compress.Writer
		uncompressed countWriter
		encoder      encoding.Encoder
	}

	dict struct {
		encoder plain.Encoder
	}

	maxValues      int32
	numValues      int32
	writePageStats bool
	isCompressed   bool
	encodings      []format.Encoding
}

func newColumnChunkWriter(writer pageWriter, columnType Type, columnIndexer ColumnIndexer, columnBufferSize int, compression compress.Codec, encoder encoding.Encoder, dataPageType format.PageType, maxRepetitionLevel, maxDefinitionLevel int8, writePageStats, isCompressed bool) *columnChunkWriter {
	ccw := &columnChunkWriter{
		writer:             writer,
		columnType:         columnType,
		columnIndexer:      columnIndexer,
		columnBufferSize:   columnBufferSize,
		compression:        compression,
		dataPageType:       dataPageType,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		writePageStats:     writePageStats,
		isCompressed:       isCompressed,
		encodings:          make([]format.Encoding, 0, 3),
	}

	if maxRepetitionLevel > 0 {
		ccw.insert = (*columnChunkWriter).insertRepeated
		ccw.commit = (*columnChunkWriter).commitRepeated
		ccw.values = make([]Value, 0, 10)
	} else {
		ccw.insert = (*columnChunkWriter).writeValues
		ccw.commit = func(*columnChunkWriter) error { return nil }
	}

	if maxDefinitionLevel > 0 {
		ccw.levels.encoder = RLE.NewEncoder(nil)
		ccw.encodings = addEncoding(ccw.encodings, format.RLE)
	}

	if isDictionaryEncoding(encoder) {
		ccw.encodings = addEncoding(ccw.encodings, format.Plain)
	}

	ccw.encodings = addEncoding(ccw.encodings, encoder.Encoding())
	ccw.page.encoder = encoder
	sortPageEncodings(ccw.encodings)
	return ccw
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

func (ccw *columnChunkWriter) clear() {
	for i := range ccw.values {
		ccw.values[i] = Value{}
	}
	ccw.values = ccw.values[:0]
}

func (ccw *columnChunkWriter) insertRepeated(values []Value) error {
	ccw.values = append(ccw.values, values...)
	return nil
}

func (ccw *columnChunkWriter) commitRepeated() error {
	return ccw.writeValues(ccw.values)
}

func (ccw *columnChunkWriter) newRowGroupColumn() RowGroupColumn {
	column := ccw.columnType.NewRowGroupColumn(ccw.columnBufferSize)
	switch {
	case ccw.maxRepetitionLevel > 0:
		column = newRepeatedRowGroupColumn(column, ccw.maxRepetitionLevel, ccw.maxDefinitionLevel, nullsGoLast)
	case ccw.maxDefinitionLevel > 0:
		column = newOptionalRowGroupColumn(column, ccw.maxDefinitionLevel, nullsGoLast)
	}
	return column
}

func (ccw *columnChunkWriter) writeValues(values []Value) error {
	if ccw.column == nil {
		// Lazily create the row group column so we don't need to allocate it if
		// only WriteRowGroup is called on the writer.
		ccw.column = ccw.newRowGroupColumn()
		ccw.maxValues = int32(ccw.column.Cap())
	}

	if ccw.numValues > 0 && (ccw.numValues+int32(len(values))) > ccw.maxValues {
		if err := ccw.flush(); err != nil {
			return err
		}
	}

	_, err := ccw.column.WriteValues(values)
	if err != nil {
		return err
	}
	ccw.numValues += int32(len(values))
	return nil
}

func (ccw *columnChunkWriter) flush() error {
	if ccw.numValues == 0 {
		return nil
	}
	defer func() {
		ccw.column.Reset()
		ccw.numValues = 0
	}()
	return ccw.writePage(ccw.writer, ccw.column.Page())
}

func (ccw *columnChunkWriter) writePage(writer pageWriter, page Page) error {
	numValues := page.NumValues()
	if numValues == 0 {
		return nil
	}

	ccw.page.buffer.Reset()
	ccw.page.checksum.Reset(&ccw.page.buffer)

	repetitionLevelsByteLength := int32(0)
	definitionLevelsByteLength := int32(0)

	if ccw.dataPageType == format.DataPageV2 {
		if ccw.maxRepetitionLevel > 0 {
			ccw.page.uncompressed.Reset(&ccw.page.checksum)
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxRepetitionLevel))
			if err := page.WriteRepetitionLevelsTo(ccw.levels.encoder); err != nil {
				return err
			}
			repetitionLevelsByteLength = int32(ccw.page.uncompressed.length)
		}
		if ccw.maxDefinitionLevel > 0 {
			ccw.page.uncompressed.Reset(&ccw.page.checksum)
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxDefinitionLevel))
			if err := page.WriteDefinitionLevelsTo(ccw.levels.encoder); err != nil {
				return err
			}
			definitionLevelsByteLength = int32(ccw.page.uncompressed.length)
		}
	}

	if !ccw.isCompressed {
		ccw.page.uncompressed.Reset(&ccw.page.checksum)
	} else {
		p, err := ccw.compressedPage(&ccw.page.checksum)
		if err != nil {
			return err
		}
		ccw.page.uncompressed.Reset(p)
	}

	if ccw.dataPageType == format.DataPage {
		if ccw.maxRepetitionLevel > 0 {
			ccw.levels.v1.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.Reset(&ccw.levels.v1)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxRepetitionLevel))
			if err := page.WriteRepetitionLevelsTo(ccw.levels.encoder); err != nil {
				return err
			}
			ccw.levels.v1.Close()
		}
		if ccw.maxDefinitionLevel > 0 {
			ccw.levels.v1.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.Reset(&ccw.levels.v1)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxDefinitionLevel))
			if err := page.WriteDefinitionLevelsTo(ccw.levels.encoder); err != nil {
				return err
			}
			ccw.levels.v1.Close()
		}
	}

	numNulls := page.NumNulls()
	minValue, maxValue := page.Bounds()
	statistics := ccw.makePageStatistics(int64(numNulls), minValue, maxValue)
	ccw.columnIndexer.IndexPage(numValues, numNulls, minValue, maxValue)

	ccw.page.encoder.Reset(&ccw.page.uncompressed)
	if err := page.WriteTo(ccw.page.encoder); err != nil {
		return err
	}
	if ccw.page.compressed != nil {
		if err := ccw.page.compressed.Close(); err != nil {
			return err
		}
	}

	ccw.header.buffer.Reset()
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(&ccw.header.buffer))
	levelsByteLength := repetitionLevelsByteLength + definitionLevelsByteLength
	uncompressedPageSize := ccw.page.uncompressed.length + int64(levelsByteLength)
	compressedPageSize := ccw.page.buffer.Len()
	encoding := ccw.page.encoder.Encoding()

	pageHeader := &format.PageHeader{
		Type:                 ccw.dataPageType,
		UncompressedPageSize: int32(uncompressedPageSize),
		CompressedPageSize:   int32(compressedPageSize),
		CRC:                  int32(ccw.page.checksum.Sum32()),
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
		return err
	}

	headerSize := ccw.header.buffer.Len()
	return writer.writePage(ccw.header.buffer.Bytes(), ccw.page.buffer.Bytes(), pageStats{
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
	ccw.page.checksum.Reset(&ccw.page.buffer)

	p, err := ccw.compressedPage(&ccw.page.checksum)
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
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(&ccw.header.buffer))

	if err := ccw.header.encoder.Encode(&format.PageHeader{
		Type:                 format.DictionaryPage,
		UncompressedPageSize: int32(ccw.page.uncompressed.length),
		CompressedPageSize:   int32(ccw.page.buffer.Len()),
		CRC:                  int32(ccw.page.checksum.Sum32()),
		DictionaryPageHeader: &format.DictionaryPageHeader{
			NumValues: int32(dict.Len()),
			Encoding:  format.Plain,
			IsSorted:  false,
		},
	}); err != nil {
		return err
	}

	headerData := ccw.header.buffer.Bytes()
	headerSize := len(headerData)

	pageData := ccw.page.buffer.Bytes()
	pageSize := len(pageData)

	return writer.writePage(headerData, pageData, pageStats{
		pageType:         format.DictionaryPage,
		encoding:         format.Plain,
		uncompressedSize: int32(headerSize) + int32(ccw.page.uncompressed.length),
		compressedSize:   int32(headerSize) + int32(pageSize),
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

func addEncoding(encodings []format.Encoding, add format.Encoding) []format.Encoding {
	for _, enc := range encodings {
		if enc == add {
			return encodings
		}
	}
	return append(encodings, add)
}

func addPageEncodingStats(stats []format.PageEncodingStats, add format.PageEncodingStats) []format.PageEncodingStats {
	for i, st := range stats {
		if st.PageType == add.PageType && st.Encoding == add.Encoding {
			stats[i].Count += add.Count
			return stats
		}
	}
	return append(stats, add)
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

type countWriter struct {
	writer io.Writer
	length int64
}

func (w *countWriter) Reset(writer io.Writer) {
	w.writer = writer
	w.length = 0
}

func (w *countWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.length += int64(n)
	return n, err
}

func (w *countWriter) WriteString(s string) (int, error) {
	n, err := io.WriteString(w.writer, s)
	w.length += int64(n)
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

type pageWriterStats struct {
	nullCount             int64
	numValues             int64
	totalRowCount         int64
	totalUncompressedSize int64
	totalCompressedSize   int64
	encodingStats         []format.PageEncodingStats
}

type pageWriter interface {
	writePage(header, data []byte, stats pageStats) error
}

type columnPageWriter interface {
	pageWriter
	reset()
	stats() *pageWriterStats
	pages() []format.PageLocation
}

type basePageWriter struct {
	index []format.PageLocation
	pageWriterStats
}

func (w *basePageWriter) observe(page pageStats) {
	switch page.pageType {
	case format.DataPage, format.DataPageV2:
		w.index = append(w.index, format.PageLocation{
			Offset:             w.totalCompressedSize,
			CompressedPageSize: page.compressedSize,
			FirstRowIndex:      w.totalRowCount,
		})
	}
	w.nullCount += int64(page.numNulls)
	w.numValues += int64(page.numValues)
	w.totalRowCount += int64(page.numRows)
	w.totalUncompressedSize += int64(page.uncompressedSize)
	w.totalCompressedSize += int64(page.compressedSize)
	w.encodingStats = addPageEncodingStats(w.encodingStats, format.PageEncodingStats{
		PageType: page.pageType,
		Encoding: page.encoding,
		Count:    1,
	})
}

func (w *basePageWriter) reset() {
	w.index = w.index[:0]
	w.pageWriterStats = pageWriterStats{encodingStats: w.encodingStats[:0]}
}

func (w *basePageWriter) stats() *pageWriterStats {
	return &w.pageWriterStats
}

func (w *basePageWriter) pages() []format.PageLocation {
	return w.index
}

type bufferedPageWriter struct {
	basePageWriter
	pool    BufferPool
	buffers []Buffer
}

func (w *bufferedPageWriter) reset() {
	for i := range w.buffers {
		w.pool.PutBuffer(w.buffers[i])
	}
	for i := range w.buffers {
		w.buffers[i] = nil
	}
	w.buffers = w.buffers[:0]
	w.basePageWriter.reset()
}

func (w *bufferedPageWriter) writePage(header, data []byte, stats pageStats) error {
	buffer := w.pool.GetBuffer()
	defer func() {
		if buffer != nil {
			w.pool.PutBuffer(buffer)
		}
	}()
	if _, err := buffer.Write(header); err != nil {
		return err
	}
	if _, err := buffer.Write(data); err != nil {
		return err
	}
	w.observe(stats)
	w.buffers = append(w.buffers, buffer)
	buffer = nil
	return nil
}

type unbufferedPageWriter struct {
	basePageWriter
	writer io.Writer
}

func (w *unbufferedPageWriter) writePage(header, data []byte, stats pageStats) error {
	if _, err := w.writer.Write(header); err != nil {
		return err
	}
	if _, err := w.writer.Write(data); err != nil {
		return err
	}
	w.observe(stats)
	return nil
}
