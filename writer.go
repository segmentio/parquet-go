package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type WriterConfig struct {
	CreatedBy string

	ColumnChunkBuffers BufferPool

	PageBufferSize int

	DataPageVersion int

	RowGroupTargetSize int64
}

func (c *WriterConfig) Apply(options ...WriterOption) {
	for _, opt := range options {
		opt.Configure(c)
	}
}

func (c *WriterConfig) Configure(config *WriterConfig) {
	*config = WriterConfig{
		CreatedBy:          coalesceString(c.CreatedBy, config.CreatedBy),
		ColumnChunkBuffers: coalesceBufferPool(c.ColumnChunkBuffers, config.ColumnChunkBuffers),
		PageBufferSize:     coalesceInt(c.PageBufferSize, config.PageBufferSize),
		DataPageVersion:    coalesceInt(c.DataPageVersion, config.DataPageVersion),
		RowGroupTargetSize: coalesceInt64(c.RowGroupTargetSize, config.RowGroupTargetSize),
	}
}

func (c *WriterConfig) Validate() error {
	const baseName = "parquet.(*WriterConfig)."
	return errorInvalidConfiguration(
		validateNotNil(baseName+"ColumnChunkBuffers", c.ColumnChunkBuffers),
		validatePositiveInt(baseName+"PageBufferSize", c.PageBufferSize),
		validatePositiveInt64(baseName+"RowGroupTargetSize", c.RowGroupTargetSize),
		validateOneOfInt(baseName+"DataPageVersion", c.DataPageVersion, 1, 2),
	)
}

type WriterOption interface {
	Configure(*WriterConfig)
}

func CreatedBy(createdBy string) WriterOption {
	return writerOption(func(config *WriterConfig) { config.CreatedBy = createdBy })
}

func ColumnChunkBuffers(buffers BufferPool) WriterOption {
	return writerOption(func(config *WriterConfig) { config.ColumnChunkBuffers = buffers })
}

func PageBufferSize(size int) WriterOption {
	return writerOption(func(config *WriterConfig) { config.PageBufferSize = size })
}

func DataPageVersion(version int) WriterOption {
	return writerOption(func(config *WriterConfig) { config.DataPageVersion = version })
}

func RowGroupTargetSize(size int64) WriterOption {
	return writerOption(func(config *WriterConfig) { config.RowGroupTargetSize = size })
}

type writerOption func(*WriterConfig)

func (opt writerOption) Configure(config *WriterConfig) { opt(config) }

type Writer struct {
	initialized bool
	closed      bool
	numRows     int64
	rowGroups   *rowGroupWriter
}

func NewWriter(writer io.Writer, schema *Schema, options ...WriterOption) *Writer {
	config := &WriterConfig{
		// default configuration
		ColumnChunkBuffers: &defaultBufferPool,
		PageBufferSize:     2 * 1024 * 1024,
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
	_, err := io.WriteString(w.rowGroups.writer, "PAR1")
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
	writer io.Writer
	schema *Schema
	config *WriterConfig

	columns   []*rowGroupColumn
	colSchema []format.SchemaElement
	rowGroups []format.RowGroup

	numRows    int64
	fileOffset int64
}

type rowGroupColumn struct {
	typ    format.Type
	codec  format.CompressionCodec
	path   []string
	buffer Buffer
	writer *columnChunkWriter

	maxDefinitionLevel int32
	maxRepetitionLevel int32

	numValues int64
}

func newRowGroupWriter(writer io.Writer, schema *Schema, config *WriterConfig) *rowGroupWriter {
	rgw := &rowGroupWriter{
		writer: writer,
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

func (rgw *rowGroupWriter) init(node Node, path []string, dataPageType format.PageType, maxRepetitionLevel, maxDefinitionLevel int32) {
	nodeType := node.Type()

	if !node.Required() {
		maxDefinitionLevel++
	}
	if node.Repeated() {
		maxRepetitionLevel++
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
		// TODO: we pick the first compression algorithm configured on the node.
		// An amelioration we could bring to this model is to generate a matrix
		// of encoding x codec and generate multiple representations of the
		// pages, picking the one with the smallest space footprint; keep it
		// simplefor now.
		compressionCodec := compress.Codec(&Uncompressed)

		for _, codec := range node.Compression() {
			if codec.CompressionCodec() != format.Uncompressed {
				compressionCodec = codec
				break
			}
		}

		buffer := rgw.config.ColumnChunkBuffers.GetBuffer()
		column := &rowGroupColumn{
			typ:                format.Type(nodeType.Kind()),
			codec:              compressionCodec.CompressionCodec(),
			path:               path,
			buffer:             buffer,
			maxRepetitionLevel: maxRepetitionLevel,
			maxDefinitionLevel: maxDefinitionLevel,
			writer: newColumnChunkWriter(
				buffer,
				compressionCodec,
				&Plain,
				dataPageType,
				maxRepetitionLevel,
				maxDefinitionLevel,
				nodeType.NewPageBuffer(rgw.config.PageBufferSize),
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
			rgw.config.ColumnChunkBuffers.PutBuffer(col.buffer)
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

	dataPageOffset := int64(4) // starts after the "PAR1" magic header
	totalByteSize := int64(0)
	totalCompressedSize := int64(0)
	columns := make([]format.ColumnChunk, len(rgw.columns))

	for _, col := range rgw.columns {
		_, err := io.Copy(rgw.writer, col.buffer)
		if err != nil {
			return err
		}
	}

	for i, col := range rgw.columns {
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
				DictionaryPageOffset:  0,
				Statistics:            col.writer.Statistics(),
				EncodingStats:         col.writer.EncodingStats(),
				BloomFilterOffset:     0,
			},
			OffsetIndexOffset: 0,
			OffsetIndexLength: 0,
			ColumnIndexOffset: 0,
			ColumnIndexLength: 0,
		}

		dataPageOffset += columnChunkTotalCompressedSize

		rgw.config.ColumnChunkBuffers.PutBuffer(col.buffer)
		col.buffer = rgw.config.ColumnChunkBuffers.GetBuffer()
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
	writer      io.Writer
	encoding    encoding.Encoding
	compression compress.Codec
	values      PageBuffer

	dataPageType       format.PageType
	maxRepetitionLevel int32
	maxDefinitionLevel int32

	levels struct {
		repetition encoding.IntArray
		definition encoding.IntArray
		encoder    encoding.Encoder
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

func newColumnChunkWriter(writer io.Writer, codec compress.Codec, enc encoding.Encoding, dataPageType format.PageType, maxRepetitionLevel, maxDefinitionLevel int32, values PageBuffer) *columnChunkWriter {
	ccw := &columnChunkWriter{
		writer:             writer,
		encoding:           enc,
		compression:        codec,
		values:             values,
		dataPageType:       dataPageType,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		isCompressed:       codec.CompressionCodec() != format.Uncompressed,
		encodings:          []format.Encoding{enc.Encoding()},
	}

	if maxRepetitionLevel > 0 {
		ccw.levels.repetition = encoding.NewFixedIntArray(bits.Len32(maxRepetitionLevel))
	}

	if maxDefinitionLevel > 0 {
		ccw.levels.definition = encoding.NewFixedIntArray(bits.Len32(maxDefinitionLevel))
	}

	if maxRepetitionLevel > 0 || maxDefinitionLevel > 0 {
		levelEncoding := encoding.Encoding(&RLE)
		if dataPageType == format.DataPageV2 {
			levelEncoding = RLE.LevelEncoding()
		}
		ccw.levels.encoder = levelEncoding.NewEncoder(nil)
	}

	ccw.page.encoder = enc.NewEncoder(nil)
	return ccw
}

func (ccw *columnChunkWriter) TotalUncompressedSize() int64 {
	return ccw.totalUncompressedSize
}

func (ccw *columnChunkWriter) TotalCompressedSize() int64 {
	return ccw.totalCompressedSize
}

func (ccw *columnChunkWriter) Encodings() []format.Encoding {
	return ccw.encodings
}

func (ccw *columnChunkWriter) EncodingStats() []format.PageEncodingStats {
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
	ccw.encodings = ccw.encodings[:1] // keep the original encoding only
	ccw.totalUncompressedSize = 0
	ccw.totalCompressedSize = 0
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
		if ccw.levels.repetition != nil {
			ccw.page.uncompressed.Reset(&ccw.page.checksum)
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len32(ccw.maxRepetitionLevel))
			ccw.levels.encoder.EncodeIntArray(ccw.levels.repetition)
			ccw.levels.encoder.Close()
			repetitionLevelsByteLength = int32(ccw.page.uncompressed.length)
		}
		if ccw.levels.definition != nil {
			ccw.page.uncompressed.Reset(&ccw.page.checksum)
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len32(ccw.maxDefinitionLevel))
			ccw.levels.encoder.EncodeIntArray(ccw.levels.definition)
			ccw.levels.encoder.Close()
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
		if ccw.levels.repetition != nil {
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len32(ccw.maxRepetitionLevel))
			ccw.levels.encoder.EncodeIntArray(ccw.levels.repetition)
			ccw.levels.encoder.Close()
		}
		if ccw.levels.definition != nil {
			ccw.levels.encoder.Reset(&ccw.page.uncompressed)
			ccw.levels.encoder.SetBitWidth(bits.Len32(ccw.maxDefinitionLevel))
			ccw.levels.encoder.EncodeIntArray(ccw.levels.definition)
			ccw.levels.encoder.Close()
		}
	}

	ccw.page.encoder.Reset(&ccw.page.uncompressed)
	if err := ccw.values.WriteTo(ccw.page.encoder); err != nil {
		return err
	}
	if err := ccw.page.encoder.Close(); err != nil {
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
	ccw.addDataPageEncodingStats(encoding)

	ccw.values.Reset()
	ccw.numNulls = 0
	ccw.numRows = 0

	if ccw.levels.repetition != nil {
		ccw.levels.repetition.Reset()
	}
	if ccw.levels.definition != nil {
		ccw.levels.definition.Reset()
	}

	if _, err := ccw.header.buffer.WriteTo(ccw.writer); err != nil {
		return err
	}
	if _, err := ccw.page.buffer.WriteTo(ccw.writer); err != nil {
		return err
	}
	return nil
}

func (ccw *columnChunkWriter) WriteValue(v Value) error {
	if ccw.levels.repetition != nil {
		ccw.levels.repetition.Append(int64(v.RepetitionLevel()))
	}

	if ccw.levels.definition != nil {
		ccw.levels.definition.Append(int64(v.DefinitionLevel()))
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

func (ccw *columnChunkWriter) addDataPageEncodingStats(encoding format.Encoding) {
	ccw.encodingStats = addPageEncodingStats(ccw.encodingStats, format.PageEncodingStats{
		PageType: ccw.dataPageType,
		Encoding: encoding,
		Count:    1,
	})
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
