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
// This example shows how to typically use parquet writers:
//
//	schema := parquet.SchemaOf(rows[0])
//	writer := parquet.NewWriter(output, schema)
//
//	for _, row := range rows {
//		if err := writer.WriteRow(row); err != nil {
//			...
//		}
//	}
//
//	if err := writer.Close(); err != nil {
//		...
//	}
//
type Writer struct {
	rowGroups   rowGroupWriter
	initialized bool
	closed      bool
}

func NewWriter(writer io.Writer, schema *Schema, options ...WriterOption) *Writer {
	config := &WriterConfig{
		// default configuration
		ColumnPageBuffers:  &defaultBufferPool,
		PageBufferSize:     1 * 1024 * 1024,
		DataPageVersion:    2,
		RowGroupTargetSize: 128 * 1024 * 1024,
	}
	config.Apply(options...)
	err := config.Validate()
	if err != nil {
		panic(err)
	}
	return &Writer{
		rowGroups: makeRowGroupWriter(writer, schema, config),
	}
}

func (w *Writer) writeMagicHeader() error {
	_, err := io.WriteString(&w.rowGroups.writer, "PAR1")
	return err
}

// Close must be called after all values were produced to the writer in order to
// flush all buffers and write the parquet footer.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if !w.initialized {
		w.initialized = true

		if err := w.writeMagicHeader(); err != nil {
			return err
		}
	}

	if err := w.rowGroups.close(); err != nil {
		return err
	}

	numRows := int64(0)
	schema := w.rowGroups.colSchema
	rowGroups := w.rowGroups.rowGroups
	createdBy := w.rowGroups.config.CreatedBy

	for rowGroupIndex := range rowGroups {
		numRows += rowGroups[rowGroupIndex].NumRows
	}

	footer, err := thrift.Marshal(new(thrift.CompactProtocol), &format.FileMetaData{
		Version:          1,
		Schema:           schema,
		NumRows:          numRows,
		RowGroups:        rowGroups,
		KeyValueMetadata: nil, // TODO
		CreatedBy:        createdBy,
		ColumnOrders:     nil, // TODO
	})
	if err != nil {
		return err
	}

	length := len(footer)
	footer = append(footer, 0, 0, 0, 0)
	footer = append(footer, "PAR1"...)
	binary.LittleEndian.PutUint32(footer[length:], uint32(length))

	_, err = w.rowGroups.writer.Write(footer)
	return err
}

// WriteRow is called to write another row to the parquet file.
//
// The method uses the parquet schema configured on w to traverse the Go value
// and decompose it into a set of columns and values.
func (w *Writer) WriteRow(row interface{}) error {
	if !w.initialized {
		w.initialized = true

		if err := w.writeMagicHeader(); err != nil {
			return err
		}
	}

	return w.rowGroups.writeRow(row)
}

type rowGroupWriter struct {
	writer countWriter
	schema *Schema
	config *WriterConfig

	columns       []*rowGroupColumn
	colSchema     []format.SchemaElement
	rowGroups     []format.RowGroup
	columnIndexes [][]format.ColumnIndex
	offsetIndexes [][]format.OffsetIndex

	numRows    int64
	fileOffset int64
}

type rowGroupColumn struct {
	typ        format.Type
	codec      format.CompressionCodec
	path       []string
	dictionary Dictionary
	buffer     *bufferPoolPageWriter
	writer     *columnChunkWriter

	maxDefinitionLevel int8
	maxRepetitionLevel int8

	numValues int64
}

func makeRowGroupWriter(writer io.Writer, schema *Schema, config *WriterConfig) rowGroupWriter {
	rgw := rowGroupWriter{
		writer: countWriter{writer: writer},
		schema: schema,
		config: config,
		// Assume this is the first row group in the file, it starts after the
		// "PAR1" magic number.
		fileOffset: 4,
	}

	dataPageType := format.DataPage
	if config.DataPageVersion == 2 {
		dataPageType = format.DataPageV2
	}

	rgw.init(schema, []string{schema.Name()}, dataPageType, 0, 0)
	return rgw
}

func (rgw *rowGroupWriter) init(node Node, path []string, dataPageType format.PageType, maxRepetitionLevel, maxDefinitionLevel int8) {
	nodeType := node.Type()

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
	logicalType := nodeType.LogicalType()
	scale, precision := (*int32)(nil), (*int32)(nil)
	if logicalType != nil && logicalType.Decimal != nil {
		scale = &logicalType.Decimal.Scale
		precision = &logicalType.Decimal.Precision
	}

	rgw.colSchema = append(rgw.colSchema, format.SchemaElement{
		Type:           nodeType.PhyiscalType(),
		TypeLength:     typeLengthOf(nodeType),
		RepetitionType: repetitionType,
		Name:           path[len(path)-1],
		NumChildren:    int32(node.NumChildren()),
		ConvertedType:  nodeType.ConvertedType(),
		Scale:          scale,
		Precision:      precision,
		LogicalType:    logicalType,
	})

	if names := node.ChildNames(); len(names) > 0 {
		base := path[:len(path):len(path)]

		for _, name := range names {
			rgw.init(node.ChildByName(name), append(base, name), dataPageType, maxRepetitionLevel, maxDefinitionLevel)
		}
	} else {
		// TODO: we pick the first encoding and compression algorithm configured
		// on the node. An amelioration we could bring to this model is to
		// generate a matrix of encoding x codec and generate multiple
		// representations of the pages, picking the one with the smallest space
		// footprint; keep it simple for now.
		encoding := encoding.Encoding(&Plain)
		compressionCodec := compress.Codec(&Uncompressed)

		for _, enc := range node.Encoding() {
			if enc.Encoding() != format.Plain {
				encoding = enc
				break
			}
		}

		for _, codec := range node.Compression() {
			if codec.CompressionCodec() != format.Uncompressed {
				compressionCodec = codec
				break
			}
		}

		dictionary := Dictionary(nil)
		pageWriter := PageWriter(nil)
		bufferSize := rgw.config.PageBufferSize
		encoder := encoding.NewEncoder(nil)

		switch encoding.Encoding() {
		case format.PlainDictionary, format.RLEDictionary:
			dictionary = nodeType.NewDictionary(bufferSize)
			pageWriter = NewIndexedPageWriter(encoder, bufferSize, dictionary)
		default:
			pageWriter = nodeType.NewPageWriter(encoder, bufferSize)
		}

		buffer := &bufferPoolPageWriter{pool: rgw.config.ColumnPageBuffers}
		column := &rowGroupColumn{
			typ:                format.Type(nodeType.Kind()),
			codec:              compressionCodec.CompressionCodec(),
			path:               path,
			dictionary:         dictionary,
			buffer:             buffer,
			maxRepetitionLevel: maxRepetitionLevel,
			maxDefinitionLevel: maxDefinitionLevel,
			writer: newColumnChunkWriter(
				buffer,
				compressionCodec,
				encoder,
				dataPageType,
				maxRepetitionLevel,
				maxDefinitionLevel,
				pageWriter,
				rgw.config.DataPageStatistics,
				// Data pages in version 2 can omit compression when dictionary
				// encoding is employed; only the dictionary page needs to be
				// compressed, the data pages are encoded with the hybrid
				// RLE/Bit-Pack encoding which doesn't benefit from an extra
				// compression layer.
				compressionCodec.CompressionCodec() != format.Uncompressed && (dataPageType != format.DataPageV2 || dictionary == nil),
			),
		}

		rgw.columns = append(rgw.columns, column)
	}
}

func (rgw *rowGroupWriter) close() error {
	if len(rgw.columns) == 0 {
		return nil
	}

	defer func() {
		for _, col := range rgw.columns {
			col.buffer.release()
		}
		rgw.columns = nil
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

	return nil
}

func (rgw *rowGroupWriter) flush() error {
	if len(rgw.columns) == 0 {
		return io.ErrClosedPipe
	}

	if rgw.numRows == 0 {
		return nil // nothing to flush
	}

	for _, col := range rgw.columns {
		if err := col.writer.flush(); err != nil {
			return err
		}
	}

	totalRowCount := int64(0)
	totalByteSize := int64(0)
	totalCompressedSize := int64(0)
	columns := make([]format.ColumnChunk, len(rgw.columns))
	columnIndex := make([]format.ColumnIndex, len(rgw.columns))
	offsetIndex := make([]format.OffsetIndex, len(rgw.columns))

	for i, col := range rgw.columns {
		dictionaryPageOffset := int64(0)
		if col.dictionary != nil {
			dictionaryPageOffset = rgw.writer.length

			if err := col.writer.writeDictionaryPage(&rgw.writer, col.dictionary); err != nil {
				return err
			}
		}

		dataPageOffset := rgw.writer.length
		columnOffsetIndex := &offsetIndex[i]
		columnOffsetIndex.PageLocations = make([]format.PageLocation, len(col.buffer.pages))

		for pageIndex := range col.buffer.pages {
			bufferPage := &col.buffer.pages[pageIndex]
			pageOffset := rgw.writer.length
			compressedPageSize, err := bufferPage.writeTo(&rgw.writer)
			if err != nil {
				return err
			}
			columnOffsetIndex.PageLocations[pageIndex] = format.PageLocation{
				Offset:             pageOffset,
				CompressedPageSize: int32(compressedPageSize),
				FirstRowIndex:      bufferPage.rowIndex,
			}
		}

		columnIndex[i] = format.ColumnIndex(col.writer.columnIndexer.ColumnIndex())
		columnChunkTotalUncompressedSize := col.writer.totalUncompressedSize
		columnChunkTotalCompressedSize := col.writer.totalCompressedSize

		totalRowCount += col.writer.totalRowCount
		totalByteSize += columnChunkTotalUncompressedSize
		totalCompressedSize += columnChunkTotalCompressedSize

		columns[i] = format.ColumnChunk{
			MetaData: format.ColumnMetaData{
				Type:                  col.typ,
				Encoding:              col.writer.sortedEncodings(),
				PathInSchema:          col.path[1:],
				Codec:                 col.codec,
				NumValues:             col.numValues,
				TotalUncompressedSize: columnChunkTotalUncompressedSize,
				TotalCompressedSize:   columnChunkTotalCompressedSize,
				KeyValueMetadata:      nil,
				DataPageOffset:        dataPageOffset,
				DictionaryPageOffset:  dictionaryPageOffset,
				Statistics:            col.writer.statistics(),
				EncodingStats:         col.writer.sortedEncodingStats(),
				BloomFilterOffset:     0,
			},
		}

		col.buffer.release()
		col.writer.reset()
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

type rowGroupTraversal struct{ *rowGroupWriter }

func (rgw rowGroupTraversal) Traverse(columnIndex int, value Value) error {
	col := rgw.columns[columnIndex]
	col.writer.writeValue(value)
	col.numValues++
	return nil
}

func (rgw *rowGroupWriter) writeRow(row interface{}) error {
	if len(rgw.columns) == 0 {
		return io.ErrClosedPipe
	}

	if err := rgw.schema.Traverse(row, rowGroupTraversal{
		rowGroupWriter: rgw,
	}); err != nil {
		return err
	}

	for _, col := range rgw.columns {
		col.writer.numRows++
	}

	rgw.numRows++

	rowGroupSize := int64(0)
	for _, col := range rgw.columns {
		rowGroupSize += col.writer.totalCompressedSize
	}
	if rowGroupSize >= rgw.config.RowGroupTargetSize {
		return rgw.flush()
	}

	return nil
}

type columnChunkWriter struct {
	buffer      pageBuffer
	values      PageWriter
	compression compress.Codec

	dataPageType       format.PageType
	maxRepetitionLevel int8
	maxDefinitionLevel int8

	levels struct {
		repetition []int8
		definition []int8
		encoder    encoding.Encoder
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

	nullCount      int64
	numNulls       int32
	numRows        int32
	writePageStats bool
	isCompressed   bool

	totalRowCount         int64
	totalUncompressedSize int64
	totalCompressedSize   int64
	encodings             []format.Encoding
	encodingStats         []format.PageEncodingStats
	columnIndexer         ColumnIndexer
}

func newColumnChunkWriter(buffer pageBuffer, codec compress.Codec, enc encoding.Encoder, dataPageType format.PageType, maxRepetitionLevel, maxDefinitionLevel int8, values PageWriter, writePageStats, isCompressed bool) *columnChunkWriter {
	typ := values.Type()
	ccw := &columnChunkWriter{
		buffer:             buffer,
		values:             values,
		compression:        codec,
		dataPageType:       dataPageType,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		writePageStats:     writePageStats,
		isCompressed:       isCompressed,
		encodings:          make([]format.Encoding, 0, 3),
		encodingStats:      make([]format.PageEncodingStats, 0, 3),
		columnIndexer:      typ.NewColumnIndexer(),
	}

	if maxRepetitionLevel > 0 {
		ccw.levels.repetition = make([]int8, 0, defaultLevelBufferSize)
	}

	if maxDefinitionLevel > 0 {
		ccw.levels.definition = make([]int8, 0, defaultLevelBufferSize)
	}

	if maxRepetitionLevel > 0 || maxDefinitionLevel > 0 {
		ccw.levels.encoder = RLE.NewEncoder(nil)
	}

	ccw.page.encoder = enc
	ccw.encodings = append(ccw.encodings, format.RLE)
	return ccw
}

func (ccw *columnChunkWriter) sortedEncodings() []format.Encoding {
	sort.Sort(columnChunkEncodingsOrder{ccw})
	return ccw.encodings
}

func (ccw *columnChunkWriter) sortedEncodingStats() []format.PageEncodingStats {
	sort.Sort(columnChunkEncodingStatsOrder{ccw})
	return ccw.encodingStats
}

func (ccw *columnChunkWriter) statistics() format.Statistics {
	min, max := ccw.columnIndexer.Bounds()
	minValue := min.Bytes()
	maxValue := max.Bytes()
	return format.Statistics{
		Min:       minValue, // deprecated
		Max:       maxValue, // deprecated
		NullCount: ccw.nullCount,
		MinValue:  minValue,
		MaxValue:  maxValue,
	}
}

func (ccw *columnChunkWriter) reset() {
	ccw.nullCount = 0
	ccw.numNulls = 0
	ccw.numRows = 0
	ccw.totalRowCount = 0
	ccw.totalUncompressedSize = 0
	ccw.totalCompressedSize = 0
	ccw.encodings = ccw.encodings[:2] // keep the original encodings only
	ccw.encodingStats = ccw.encodingStats[:0]
	ccw.columnIndexer.Reset()
}

func (ccw *columnChunkWriter) flush() error {
	numValues := ccw.values.NumValues()
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
			ccw.levels.encoder.EncodeInt8(ccw.levels.repetition)
			repetitionLevelsByteLength = int32(ccw.page.uncompressed.length)
		}
		if ccw.maxDefinitionLevel > 0 {
			ccw.page.uncompressed.Reset(&ccw.page.checksum)
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxDefinitionLevel))
			ccw.levels.encoder.EncodeInt8(ccw.levels.definition)
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
			ccw.levels.encoder.EncodeInt8(ccw.levels.repetition)
			ccw.levels.v1.Close()
		}
		if ccw.maxDefinitionLevel > 0 {
			ccw.levels.v1.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.Reset(&ccw.levels.v1)
			ccw.levels.encoder.SetBitWidth(bits.Len8(ccw.maxDefinitionLevel))
			ccw.levels.encoder.EncodeInt8(ccw.levels.definition)
			ccw.levels.v1.Close()
		}
	}

	minValue, maxValue := ccw.values.Bounds()
	statistics := ccw.makePageStatistics(minValue, maxValue)
	ccw.columnIndexer.IndexPage(numValues, int(ccw.numNulls), minValue, maxValue)

	ccw.page.encoder.Reset(&ccw.page.uncompressed)
	if err := ccw.values.Flush(); err != nil {
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

	switch ccw.dataPageType {
	case format.DataPage:
		pageHeader.DataPageHeader = &format.DataPageHeader{
			NumValues:               int32(numValues) + ccw.numNulls,
			Encoding:                encoding,
			DefinitionLevelEncoding: format.RLE,
			RepetitionLevelEncoding: format.RLE,
			Statistics:              statistics,
		}
	case format.DataPageV2:
		pageHeader.DataPageHeaderV2 = &format.DataPageHeaderV2{
			NumValues:                  int32(numValues) + ccw.numNulls,
			NumNulls:                   ccw.numNulls,
			NumRows:                    ccw.numRows,
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

	rowIndex := ccw.totalRowCount
	headerSize := ccw.header.buffer.Len()
	ccw.totalRowCount += int64(ccw.numRows)
	ccw.totalUncompressedSize += int64(headerSize) + int64(uncompressedPageSize)
	ccw.totalCompressedSize += int64(headerSize) + int64(compressedPageSize)
	ccw.addPageEncoding(ccw.dataPageType, encoding)

	ccw.values.Reset(ccw.page.encoder)
	ccw.numNulls = 0
	ccw.numRows = 0
	ccw.levels.repetition = ccw.levels.repetition[:0]
	ccw.levels.definition = ccw.levels.definition[:0]

	return ccw.buffer.writePage(rowIndex, ccw.header.buffer.Bytes(), ccw.page.buffer.Bytes())
}

func (ccw *columnChunkWriter) writeValue(v Value) error {
	if ccw.maxRepetitionLevel > 0 {
		ccw.levels.repetition = append(ccw.levels.repetition, v.repetitionLevel)
	}

	if ccw.maxDefinitionLevel > 0 {
		ccw.levels.definition = append(ccw.levels.definition, v.definitionLevel)
	}

	if v.IsNull() {
		ccw.nullCount++
		ccw.numNulls++
		return nil
	}

	for {
		switch err := ccw.values.WriteValue(v); err {
		case nil:
			return nil
		case ErrBufferFull:
			if err := ccw.flush(); err != nil {
				return err
			}
		default:
			return err
		}
	}
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

func (ccw *columnChunkWriter) writeDictionaryPage(w io.Writer, dict Dictionary) error {
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

	headerSize := ccw.header.buffer.Len()
	ccw.totalUncompressedSize += int64(headerSize) + ccw.page.uncompressed.length
	ccw.totalCompressedSize += int64(headerSize) + int64(ccw.page.buffer.Len())
	ccw.addPageEncoding(format.DictionaryPage, format.Plain)

	if _, err := ccw.header.buffer.WriteTo(w); err != nil {
		return err
	}
	if _, err := ccw.page.buffer.WriteTo(w); err != nil {
		return err
	}
	return nil
}

func (ccw *columnChunkWriter) makePageStatistics(minValue, maxValue Value) (stats format.Statistics) {
	if ccw.writePageStats {
		minValueBytes := minValue.Bytes()
		maxValueBytes := maxValue.Bytes()
		stats = format.Statistics{
			Min:       minValueBytes, // deprecated
			Max:       maxValueBytes, // deprecated
			NullCount: int64(ccw.numNulls),
			MinValue:  minValueBytes,
			MaxValue:  maxValueBytes,
		}
	}
	return stats
}

func (ccw *columnChunkWriter) addPageEncoding(pageType format.PageType, encoding format.Encoding) {
	ccw.encodings = addEncoding(ccw.encodings, encoding)
	ccw.encodingStats = addPageEncodingStats(ccw.encodingStats, format.PageEncodingStats{
		PageType: pageType,
		Encoding: encoding,
		Count:    1,
	})
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

type columnChunkEncodingsOrder struct{ *columnChunkWriter }

func (c columnChunkEncodingsOrder) Len() int {
	return len(c.encodings)
}
func (c columnChunkEncodingsOrder) Less(i, j int) bool {
	return c.encodings[i] < c.encodings[j]
}
func (c columnChunkEncodingsOrder) Swap(i, j int) {
	c.encodings[i], c.encodings[j] = c.encodings[j], c.encodings[i]
}

type columnChunkEncodingStatsOrder struct{ *columnChunkWriter }

func (c columnChunkEncodingStatsOrder) Len() int {
	return len(c.encodingStats)
}
func (c columnChunkEncodingStatsOrder) Less(i, j int) bool {
	s1 := &c.encodingStats[i]
	s2 := &c.encodingStats[j]
	if s1.PageType != s2.PageType {
		return s1.PageType < s2.PageType
	}
	return s1.Encoding < s2.Encoding
}
func (c columnChunkEncodingStatsOrder) Swap(i, j int) {
	c.encodingStats[i], c.encodingStats[j] = c.encodingStats[j], c.encodingStats[i]
}

type pageBuffer interface {
	writePage(rowIndex int64, header, data []byte) error
}

type bufferPageWriter struct {
	buffer   Buffer
	rowIndex int64
}

func (w *bufferPageWriter) writePage(rowIndex int64, header, data []byte) error {
	if _, err := w.buffer.Write(header); err != nil {
		return err
	}
	if _, err := w.buffer.Write(data); err != nil {
		return err
	}
	w.rowIndex = rowIndex
	return nil
}

func (w *bufferPageWriter) writeTo(dst io.Writer) (int64, error) {
	return io.Copy(dst, w.buffer)
}

func (w *bufferPageWriter) release(pool BufferPool) {
	if buf := w.buffer; buf != nil {
		w.buffer = nil
		pool.PutBuffer(buf)
	}
}

type bufferPoolPageWriter struct {
	pool  BufferPool
	pages []bufferPageWriter
}

func (w *bufferPoolPageWriter) writePage(rowIndex int64, header, data []byte) error {
	writer := bufferPageWriter{buffer: w.pool.GetBuffer()}
	if err := writer.writePage(rowIndex, header, data); err != nil {
		return err
	}
	w.pages = append(w.pages, writer)
	return nil
}

func (w *bufferPoolPageWriter) writeTo(dst io.Writer) (int64, error) {
	size := int64(0)
	for _, page := range w.pages {
		n, err := page.writeTo(dst)
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func (w *bufferPoolPageWriter) release() {
	for i := range w.pages {
		w.pages[i].release(w.pool)
	}
	w.pages = w.pages[:0]
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
