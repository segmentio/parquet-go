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
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type Writer struct {
	initialized bool
	closed      bool
	numRows     int64
	rowGroups   *rowGroupWriter
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
		rowGroups: newRowGroupWriter(writer, schema, config),
	}
}

func (w *Writer) writeMagicHeader() error {
	_, err := io.WriteString(&w.rowGroups.writer, "PAR1")
	return err
}

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

	if err := w.rowGroups.Close(); err != nil {
		return err
	}

	footer, err := thrift.Marshal(new(thrift.CompactProtocol), &format.FileMetaData{
		Version:          1,
		Schema:           w.rowGroups.Schema(),
		NumRows:          w.numRows,
		RowGroups:        w.rowGroups.RowGroups(),
		KeyValueMetadata: nil, // TODO
		CreatedBy:        w.rowGroups.config.CreatedBy,
		ColumnOrders:     nil, // TODOEncr
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

func (w *Writer) WriteRow(row interface{}) error {
	if !w.initialized {
		w.initialized = true

		if err := w.writeMagicHeader(); err != nil {
			return err
		}
	}

	if err := w.rowGroups.WriteRow(row); err != nil {
		return err
	}

	w.numRows++
	return nil
}

type rowGroupWriter struct {
	writer countWriter
	schema *Schema
	config *WriterConfig

	columns   []*rowGroupColumn
	colSchema []format.SchemaElement
	rowGroups []format.RowGroup

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

func newRowGroupWriter(writer io.Writer, schema *Schema, config *WriterConfig) *rowGroupWriter {
	rgw := &rowGroupWriter{
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

	rgw.colSchema = append(rgw.colSchema, format.SchemaElement{
		Type:           nodeType.PhyiscalType(),
		TypeLength:     typeLengthOf(nodeType),
		RepetitionType: repetitionType,
		Name:           path[len(path)-1],
		NumChildren:    int32(node.NumChildren()),
		ConvertedType:  nodeType.ConvertedType(),
		LogicalType:    nodeType.LogicalType(),
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
		pageBuffer := PageBuffer(nil)
		bufferSize := rgw.config.PageBufferSize

		switch encoding.Encoding() {
		case format.PlainDictionary, format.RLEDictionary:
			bufferSize /= 2
			dictionary = nodeType.NewDictionary(bufferSize)
			pageBuffer = NewDictionaryPageBuffer(dictionary, bufferSize)
		default:
			pageBuffer = nodeType.NewPageBuffer(bufferSize)
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
				encoding,
				dataPageType,
				maxRepetitionLevel,
				maxDefinitionLevel,
				pageBuffer,
			),
		}

		rgw.columns = append(rgw.columns, column)
	}
}

func (rgw *rowGroupWriter) RowGroups() []format.RowGroup {
	return rgw.rowGroups
}

func (rgw *rowGroupWriter) Schema() []format.SchemaElement {
	return rgw.colSchema
}

func (rgw *rowGroupWriter) Close() error {
	var err error

	if len(rgw.columns) > 0 {
		err = rgw.Flush()

		for _, col := range rgw.columns {
			col.buffer.release()
		}

		rgw.columns = nil
	}

	return err
}

func (rgw *rowGroupWriter) Flush() error {
	if len(rgw.columns) == 0 {
		return io.ErrClosedPipe
	}

	if rgw.numRows == 0 {
		return nil // nothing to flush
	}

	for _, col := range rgw.columns {
		if err := col.writer.Flush(); err != nil {
			return err
		}
	}

	totalByteSize := int64(0)
	totalCompressedSize := int64(0)
	columns := make([]format.ColumnChunk, len(rgw.columns))

	for i, col := range rgw.columns {
		dictionaryPageOffset := int64(0)
		if col.dictionary != nil {
			dictionaryPageOffset = rgw.writer.length

			if err := col.writer.writeDictionaryPage(&rgw.writer, col.dictionary); err != nil {
				return err
			}
		}

		dataPageOffset := rgw.writer.length
		if err := col.buffer.writeTo(&rgw.writer); err != nil {
			return err
		}

		columnChunkTotalUncompressedSize := col.writer.TotalUncompressedSize()
		columnChunkTotalCompressedSize := col.writer.TotalCompressedSize()

		totalByteSize += columnChunkTotalUncompressedSize
		totalCompressedSize += columnChunkTotalCompressedSize

		columns[i] = format.ColumnChunk{
			MetaData: format.ColumnMetaData{
				Type:                  col.typ,
				Encoding:              col.writer.Encodings(),
				PathInSchema:          col.path[1:],
				Codec:                 col.codec,
				NumValues:             col.numValues,
				TotalUncompressedSize: columnChunkTotalUncompressedSize,
				TotalCompressedSize:   columnChunkTotalCompressedSize,
				KeyValueMetadata:      nil,
				DataPageOffset:        dataPageOffset,
				IndexPageOffset:       0,
				DictionaryPageOffset:  dictionaryPageOffset,
				Statistics:            col.writer.Statistics(),
				EncodingStats:         col.writer.EncodingStats(),
				BloomFilterOffset:     0,
			},
			OffsetIndexOffset: 0,
			OffsetIndexLength: 0,
			ColumnIndexOffset: 0,
			ColumnIndexLength: 0,
		}

		col.buffer.release()
		col.writer.Reset()
	}

	rgw.rowGroups = append(rgw.rowGroups, format.RowGroup{
		Columns:             columns,
		TotalByteSize:       totalByteSize,
		NumRows:             rgw.numRows,
		SortingColumns:      nil, // TODO
		FileOffset:          rgw.fileOffset,
		TotalCompressedSize: totalCompressedSize,
		Ordinal:             int16(len(rgw.rowGroups)),
	})

	rgw.numRows = 0
	rgw.fileOffset += totalCompressedSize
	return nil
}

type rowGroupTraversal struct{ *rowGroupWriter }

func (rgw rowGroupTraversal) Traverse(columnIndex int, value Value) error {
	col := rgw.columns[columnIndex]
	col.writer.WriteValue(value)
	col.numValues++
	return nil
}

func (rgw *rowGroupWriter) WriteRow(row interface{}) error {
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
		rowGroupSize += col.writer.TotalCompressedSize()
	}
	if rowGroupSize >= rgw.config.RowGroupTargetSize {
		return rgw.Flush()
	}

	return nil
}

type columnChunkWriter struct {
	writer      pageWriter
	encoding    encoding.Encoding
	compression compress.Codec
	values      PageBuffer

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

	minValueBytes []byte
	maxValueBytes []byte
	minValue      Value
	maxValue      Value
	nullCount     int64
	numNulls      int32
	numRows       int32
	isCompressed  bool

	totalUncompressedSize int64
	totalCompressedSize   int64
	encodings             []format.Encoding
	encodingStats         []format.PageEncodingStats
}

func newColumnChunkWriter(writer pageWriter, codec compress.Codec, enc encoding.Encoding, dataPageType format.PageType, maxRepetitionLevel, maxDefinitionLevel int8, values PageBuffer) *columnChunkWriter {
	ccw := &columnChunkWriter{
		writer:             writer,
		encoding:           enc,
		compression:        codec,
		values:             values,
		dataPageType:       dataPageType,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		isCompressed:       codec.CompressionCodec() != format.Uncompressed,
		encodings:          make([]format.Encoding, 0, 3),
		encodingStats:      make([]format.PageEncodingStats, 0, 3),
	}

	if maxRepetitionLevel > 0 {
		ccw.levels.repetition = make([]int8, 0, 1024)
	}

	if maxDefinitionLevel > 0 {
		ccw.levels.definition = make([]int8, 0, 1024)
	}

	if maxRepetitionLevel > 0 || maxDefinitionLevel > 0 {
		ccw.levels.encoder = RLE.NewEncoder(nil)
	}

	ccw.page.encoder = enc.NewEncoder(nil)
	ccw.encodings = append(ccw.encodings, format.RLE)
	return ccw
}

func (ccw *columnChunkWriter) TotalUncompressedSize() int64 {
	return ccw.totalUncompressedSize
}

func (ccw *columnChunkWriter) TotalCompressedSize() int64 {
	return ccw.totalCompressedSize
}

func (ccw *columnChunkWriter) Encodings() []format.Encoding {
	sort.Sort(columnChunkEncodingsOrder{ccw})
	return ccw.encodings
}

func (ccw *columnChunkWriter) EncodingStats() []format.PageEncodingStats {
	sort.Sort(columnChunkEncodingStatsOrder{ccw})
	return ccw.encodingStats
}

func (ccw *columnChunkWriter) Statistics() format.Statistics {
	ccw.minValueBytes = ccw.minValue.AppendBytes(ccw.minValueBytes[:0])
	ccw.maxValueBytes = ccw.maxValue.AppendBytes(ccw.maxValueBytes[:0])
	return format.Statistics{
		Min:       ccw.minValueBytes, // deprecated
		Max:       ccw.maxValueBytes, // deprecated
		NullCount: ccw.nullCount,
		MinValue:  ccw.minValueBytes,
		MaxValue:  ccw.maxValueBytes,
	}
}

func (ccw *columnChunkWriter) Reset() {
	ccw.minValueBytes = ccw.minValueBytes[:0]
	ccw.maxValueBytes = ccw.maxValueBytes[:0]
	ccw.minValue = Value{}
	ccw.maxValue = Value{}
	ccw.nullCount = 0
	ccw.numNulls = 0
	ccw.numRows = 0
	ccw.totalUncompressedSize = 0
	ccw.totalCompressedSize = 0
	ccw.encodings = ccw.encodings[:2] // keep the original encodings only
	ccw.encodingStats = ccw.encodingStats[:0]
}

func (ccw *columnChunkWriter) Flush() error {
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

	if ccw.page.compressed == nil {
		w, err := ccw.compression.NewWriter(&ccw.page.checksum)
		if err != nil {
			return fmt.Errorf("creating compressor for parquet column chunk writer: %w", err)
		}
		ccw.page.compressed = w
	} else {
		if err := ccw.page.compressed.Reset(&ccw.page.checksum); err != nil {
			return fmt.Errorf("resetting compressor for parquet column chunk writer: %w", err)
		}
	}

	ccw.page.uncompressed.Reset(ccw.page.compressed)
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

	ccw.page.encoder.Reset(&ccw.page.uncompressed)
	if err := ccw.values.WriteTo(ccw.page.encoder); err != nil {
		return err
	}
	if err := ccw.page.compressed.Close(); err != nil {
		return err
	}

	ccw.header.buffer.Reset()
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(&ccw.header.buffer))
	levelsByteLength := repetitionLevelsByteLength + definitionLevelsByteLength
	uncompressedPageSize := ccw.page.uncompressed.length + int64(levelsByteLength)
	compressedPageSize := ccw.page.buffer.Len()
	encoding := ccw.encoding.Encoding()

	pageHeader := &format.PageHeader{
		Type:                 ccw.dataPageType,
		UncompressedPageSize: int32(uncompressedPageSize),
		CompressedPageSize:   int32(compressedPageSize),
		CRC:                  int32(ccw.page.checksum.Sum32()),
	}

	minValue, maxValue := ccw.values.Bounds()
	ccw.minValueBytes = minValue.AppendBytes(ccw.minValueBytes[:0])
	ccw.maxValueBytes = maxValue.AppendBytes(ccw.maxValueBytes[:0])

	statistics := format.Statistics{
		Min:           ccw.minValueBytes, // deprecated
		Max:           ccw.maxValueBytes, // deprecated
		NullCount:     int64(ccw.numNulls),
		DistinctCount: int64(ccw.values.DistinctCount()),
		MinValue:      ccw.minValueBytes,
		MaxValue:      ccw.maxValueBytes,
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

	typ := ccw.values.Type()
	if ccw.minValue.IsNull() || typ.Less(minValue, ccw.minValue) {
		ccw.minValue = minValue
	}
	if ccw.maxValue.IsNull() || typ.Less(ccw.maxValue, maxValue) {
		ccw.maxValue = maxValue
	}

	headerSize := ccw.header.buffer.Len()
	ccw.totalUncompressedSize += int64(headerSize) + int64(uncompressedPageSize)
	ccw.totalCompressedSize += int64(headerSize) + int64(compressedPageSize)
	ccw.addPageEncoding(ccw.dataPageType, encoding)

	ccw.values.Reset()
	ccw.numNulls = 0
	ccw.numRows = 0
	ccw.levels.repetition = ccw.levels.repetition[:0]
	ccw.levels.definition = ccw.levels.definition[:0]

	return ccw.writer.writePage(ccw.header.buffer.Bytes(), ccw.page.buffer.Bytes())
}

func (ccw *columnChunkWriter) WriteValue(v Value) error {
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
			if err := ccw.Flush(); err != nil {
				return err
			}
		default:
			return err
		}
	}
}

func (ccw *columnChunkWriter) writeDictionaryPage(w io.Writer, dict Dictionary) error {
	ccw.page.buffer.Reset()
	ccw.page.checksum.Reset(&ccw.page.buffer)

	if err := ccw.page.compressed.Reset(&ccw.page.checksum); err != nil {
		return fmt.Errorf("resetting compressor for parquet column chunk writer: %w", err)
	}
	keys := dict.Keys()
	if _, err := ccw.page.compressed.Write(keys); err != nil {
		return err
	}
	if err := ccw.page.compressed.Close(); err != nil {
		return err
	}

	ccw.header.buffer.Reset()
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(&ccw.header.buffer))

	if err := ccw.header.encoder.Encode(&format.PageHeader{
		Type:                 format.DictionaryPage,
		UncompressedPageSize: int32(len(keys)),
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
	ccw.totalUncompressedSize += int64(headerSize) + int64(len(keys))
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

type pageWriter interface {
	writePage(header, data []byte) error
}

type bufferPageWriter struct {
	buffer Buffer
}

func (w *bufferPageWriter) writePage(header, data []byte) error {
	if _, err := w.buffer.Write(header); err != nil {
		return err
	}
	if _, err := w.buffer.Write(data); err != nil {
		return err
	}
	return nil
}

func (w *bufferPageWriter) writeTo(dst io.Writer) error {
	_, err := io.Copy(dst, w.buffer)
	return err
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

func (w *bufferPoolPageWriter) writePage(header, data []byte) error {
	writer := bufferPageWriter{buffer: w.pool.GetBuffer()}
	if err := writer.writePage(header, data); err != nil {
		return err
	}
	w.pages = append(w.pages, writer)
	return nil
}

func (w *bufferPoolPageWriter) writeTo(dst io.Writer) error {
	for _, page := range w.pages {
		if err := page.writeTo(dst); err != nil {
			return err
		}
	}
	return nil
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
