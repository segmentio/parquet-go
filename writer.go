package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
)

type WriterConfig struct {
	CreatedBy string

	ColumnChunkBuffers BufferPool

	PageBufferSize int

	RowGroupTargetSize int64
}

func (cfg *WriterConfig) Apply(options ...WriterOption) {
	for _, opt := range options {
		opt.Configure(cfg)
	}
}

func (cfg *WriterConfig) Configure(config *WriterConfig) {
	if cfg.CreatedBy != "" {
		config.CreatedBy = cfg.CreatedBy
	}
	if cfg.ColumnChunkBuffers != nil {
		config.ColumnChunkBuffers = cfg.ColumnChunkBuffers
	}
	if cfg.PageBufferSize > 0 {
		config.PageBufferSize = cfg.PageBufferSize
	}
	if cfg.RowGroupTargetSize > 0 {
		config.RowGroupTargetSize = cfg.RowGroupTargetSize
	}
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
	return &Writer{
		rowGroups: newRowGroupWriter(writer, schema, options...),
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
	object Object
	row    Row

	columns   []*rowGroupColumn
	schema    []format.SchemaElement
	rowGroups []format.RowGroup

	numRows    int64
	fileOffset int64

	config WriterConfig
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

func newRowGroupWriter(writer io.Writer, schema *Schema, options ...WriterOption) *rowGroupWriter {
	rgw := &rowGroupWriter{
		writer: writer,
		object: schema.Object(reflect.Value{}),
		// Assume this is the first row group in the file, it starts after the
		// "PAR1" magic number.
		fileOffset: 4,
		config: WriterConfig{
			// default configuration
			ColumnChunkBuffers: &defaultBufferPool,
			PageBufferSize:     2 * 1024 * 1024,
			RowGroupTargetSize: 128 * 1024 * 1024,
		},
	}
	rgw.config.Apply(options...)
	rgw.init(schema, []string{schema.Name()}, 0, 0)
	return rgw
}

func (rgw *rowGroupWriter) init(node Node, path []string, maxRepetitionLevel, maxDefinitionLevel int32) {
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

	rgw.schema = append(rgw.schema, format.SchemaElement{
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
			rgw.init(node.ChildByName(name), append(base, name), maxRepetitionLevel, maxDefinitionLevel)
		}
	} else {

		column := &rowGroupColumn{
			typ:                format.Type(nodeType.Kind()),
			codec:              Uncompressed.CompressionCodec(),
			path:               path,
			buffer:             rgw.config.ColumnChunkBuffers.GetBuffer(),
			maxRepetitionLevel: maxRepetitionLevel,
			maxDefinitionLevel: maxDefinitionLevel,
		}

		column.writer = newColumnChunkWriter(
			column.buffer,
			&Uncompressed,
			&Plain,
			maxRepetitionLevel,
			maxDefinitionLevel,
			nodeType.NewPageBuffer(rgw.config.PageBufferSize),
		)

		rgw.columns = append(rgw.columns, column)
	}
}

func (rgw *rowGroupWriter) RowGroups() []format.RowGroup {
	return rgw.rowGroups
}

func (rgw *rowGroupWriter) Schema() []format.SchemaElement {
	return rgw.schema
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

func (rgw *rowGroupWriter) WriteRow(row interface{}) error {
	if len(rgw.columns) == 0 {
		return io.ErrClosedPipe
	}

	rgw.object.Reset(reflect.ValueOf(row))
	rgw.row.Reset(rgw.object)

	for rgw.row.Next() {
		val := rgw.row.Value()
		col := rgw.columns[rgw.row.ColumnIndex()]
		col.writer.WriteValue(val)
		col.numValues++
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

	levels struct {
		repetition []int32
		definition []int32
		encoder    rle.LevelEncoder
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

	minValue  Value
	maxValue  Value
	nullCount int64
	numNulls  int32
	numRows   int32

	totalUncompressedSize int64
	totalCompressedSize   int64
	encodings             []format.Encoding
	encodingStats         []format.PageEncodingStats
}

func newColumnChunkWriter(writer io.Writer, compression compress.Codec, encoding encoding.Encoding, maxRepetitionLevel, maxDefinitionLevel int32, values PageBuffer) *columnChunkWriter {
	ccw := &columnChunkWriter{
		writer:      writer,
		encoding:    encoding,
		compression: compression,
		values:      values,
		encodings:   []format.Encoding{encoding.Encoding()},
	}
	if maxRepetitionLevel > 0 {
		ccw.levels.repetition = make([]int32, 0, 1024)
	}
	if maxDefinitionLevel > 0 {
		ccw.levels.definition = make([]int32, 0, 1024)
	}
	ccw.page.encoder = encoding.NewEncoder(nil)
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
	minValue := ccw.minValue.Bytes()
	maxValue := ccw.maxValue.Bytes()
	return format.Statistics{
		MinValue:  minValue,
		MaxValue:  maxValue,
		NullCount: ccw.nullCount,
		Min:       minValue, // deprecated
		Max:       maxValue, // deprecated
	}
}

func (ccw *columnChunkWriter) Reset() {
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
	defer func() {
		ccw.values.Reset()
		ccw.levels.repetition = ccw.levels.repetition[:0]
		ccw.levels.definition = ccw.levels.definition[:0]
	}()

	ccw.page.buffer.Reset()
	ccw.page.checksum.Reset(&ccw.page.buffer)

	repetitionLevelsByteLength := int32(0)
	definitionLevelsByteLength := int32(0)

	if len(ccw.levels.repetition) > 0 {
		ccw.page.uncompressed.Reset(&ccw.page.checksum)
		ccw.levels.encoder.Reset(&ccw.page.uncompressed)
		ccw.levels.encoder.EncodeInt32(ccw.levels.repetition)
		ccw.levels.encoder.Close()
		repetitionLevelsByteLength = int32(ccw.page.uncompressed.length)
	}

	if len(ccw.levels.definition) > 0 {
		ccw.page.uncompressed.Reset(&ccw.page.checksum)
		ccw.levels.encoder.Reset(&ccw.page.uncompressed)
		ccw.levels.encoder.EncodeInt32(ccw.levels.definition)
		ccw.levels.encoder.Close()
		definitionLevelsByteLength = int32(ccw.page.uncompressed.length)
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
	ccw.page.encoder.Reset(&ccw.page.uncompressed)

	if err := ccw.values.WriteTo(ccw.page.encoder); err != nil {
		return err
	}
	if err := ccw.page.encoder.Close(); err != nil {
		return err
	}

	ccw.header.buffer.Reset()
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(&ccw.header.buffer))
	encoding := ccw.encoding.Encoding()

	minValue, maxValue := ccw.values.Bounds()
	minValueBytes := minValue.Bytes()
	maxValueBytes := maxValue.Bytes()

	if err := ccw.header.encoder.Encode(&format.PageHeader{
		Type:                 format.DataPageV2,
		UncompressedPageSize: int32(ccw.page.uncompressed.length),
		CompressedPageSize:   int32(ccw.page.buffer.Len()),
		CRC:                  int32(ccw.page.checksum.Sum32()),
		DataPageHeaderV2: &format.DataPageHeaderV2{
			NumValues:                  int32(numValues),
			NumNulls:                   ccw.numNulls,
			NumRows:                    ccw.numRows,
			Encoding:                   encoding,
			DefinitionLevelsByteLength: definitionLevelsByteLength,
			RepetitionLevelsByteLength: repetitionLevelsByteLength,
			Statistics: format.Statistics{
				Min:           minValueBytes, // deprecated
				Max:           maxValueBytes, // deprecated
				NullCount:     int64(ccw.numNulls),
				DistinctCount: int64(ccw.values.DistinctCount()),
				MinValue:      minValueBytes,
				MaxValue:      maxValueBytes,
			},
		},
	}); err != nil {
		return err
	}

	typ := ccw.values.Type()
	if ccw.minValue.IsNull() || typ.Less(minValue, ccw.minValue) {
		ccw.minValue = minValue
	}
	if ccw.maxValue.IsNull() || typ.Less(ccw.maxValue, maxValue) {
		ccw.maxValue = maxValue
	}

	headerSize := int64(ccw.header.buffer.Len())
	uncompressedPageSize := ccw.page.uncompressed.length
	compressedPageSize := int64(ccw.page.buffer.Len())

	ccw.totalUncompressedSize += headerSize + uncompressedPageSize
	ccw.totalCompressedSize += headerSize + compressedPageSize
	ccw.encodingStats = addPageEncodingStats(ccw.encodingStats, format.PageEncodingStats{
		PageType: format.DataPageV2,
		Encoding: encoding,
		Count:    1,
	})

	ccw.numNulls = 0
	ccw.numRows = 0

	if _, err := ccw.header.buffer.WriteTo(ccw.writer); err != nil {
		return err
	}
	if _, err := ccw.page.buffer.WriteTo(ccw.writer); err != nil {
		return err
	}
	return nil
}

func (ccw *columnChunkWriter) WriteValue(v Value) error {
	if cap(ccw.levels.repetition) > 0 {
		ccw.levels.repetition = append(ccw.levels.repetition, v.RepetitionLevel())
	}

	if cap(ccw.levels.definition) > 0 {
		ccw.levels.definition = append(ccw.levels.definition, v.DefinitionLevel())
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
