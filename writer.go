package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
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

	numNulls     int32
	numRows      int32
	isCompressed bool

	totalUncompressedSize int64
	totalCompressedSize   int64
	encodings             []format.Encoding
	encodingStats         []format.PageEncodingStats
	statistics            format.Statistics
	distinctCount         *hll.Sketch
}

type pageTypeEncoding struct {
	pageType format.PageType
	encoding format.Encoding
}

func newColumnChunkWriter(writer io.Writer, compression compress.Codec, encoding encoding.Encoding, values PageBuffer) *columnChunkWriter {
	return &columnChunkWriter{
		writer:        writer,
		encoding:      encoding,
		compression:   compression,
		values:        values,
		isCompressed:  true,
		encodings:     []format.Encoding{encoding.Encoding()},
		distinctCount: hll.New(),
	}
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
	ccw.statistics.Min = ccw.statistics.MinValue // deprecated
	ccw.statistics.Max = ccw.statistics.MaxValue // depreacted
	ccw.statistics.DistinctCount = int64(ccw.distinctCount.Estimate())
	if ccw.statistics.NullCount > 0 {
		ccw.statistics.DistinctCount++
	}
	return ccw.statistics
}

func (ccw *columnChunkWriter) Reset() {
	ccw.numNulls = 0
	ccw.numRows = 0
	ccw.encodings = ccw.encodings[:1] // keep the original encoding only
	ccw.totalUncompressedSize = 0
	ccw.totalCompressedSize = 0
	ccw.encodingStats = ccw.encodingStats[:0]
	ccw.statistics = format.Statistics{}
}

func (ccw *columnChunkWriter) Flush() error {
	ccw.page.buffer.Reset()
	ccw.page.checksum.Reset(&ccw.page.buffer)

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

	if ccw.page.encoder == nil {
		ccw.page.encoder = ccw.encoding.NewEncoder(&ccw.page.uncompressed)
	} else {
		ccw.page.encoder.Reset(&ccw.page.uncompressed)
	}

	minValue, maxValue := ccw.values.Bounds()
	numValues, distinctCount, err := ccw.values.WriteTo(ccw.page.encoder)
	if err != nil {
		return err
	}
	if numValues == 0 {
		return nil
	}

	ccw.header.buffer.Reset()
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(&ccw.header.buffer))
	encoding := ccw.encoding.Encoding()

	if err := ccw.header.encoder.Encode(&format.PageHeader{
		Type:                 format.DataPageV2,
		UncompressedPageSize: int32(ccw.page.uncompressed.length),
		CompressedPageSize:   int32(ccw.page.buffer.Len()),
		CRC:                  int32(ccw.page.checksum.Sum32()),
		DataPageHeaderV2: &format.DataPageHeaderV2{
			NumValues: int32(numValues),
			NumNulls:  ccw.numNulls,
			NumRows:   ccw.numRows,
			Encoding:  encoding,
			// DefinitionLevelsByteLength:
			// RepetitionLevelsByteLength:
			IsCompressed: &ccw.isCompressed,
			Statistics: format.Statistics{
				Min:           minValue, // deprecated
				Max:           maxValue, // deprecated
				NullCount:     int64(ccw.numNulls),
				DistinctCount: int64(distinctCount),
				MinValue:      minValue,
				MaxValue:      maxValue,
			},
		},
	}); err != nil {
		return err
	}

	if ccw.statistics.MinValue == nil || ccw.values.Less(minValue, ccw.statistics.MinValue) {
		ccw.statistics.MinValue = append(ccw.statistics.MinValue[:0], minValue...)
	}
	if ccw.statistics.MaxValue == nil || ccw.values.Less(ccw.statistics.MaxValue, maxValue) {
		ccw.statistics.MaxValue = append(ccw.statistics.MaxValue[:0], maxValue...)
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
	for {
		switch err := ccw.values.WriteValue(v); err {
		case nil:
			switch v.Kind() {
			case Boolean:
				hash := uint64(0)
				if v.Boolean() {
					hash = 1
				}
				ccw.distinctCount.InsertHash(hash)

			case Int32:
				ccw.distinctCount.InsertHash(uint64(v.Int32()))

			case Int64:
				ccw.distinctCount.InsertHash(uint64(v.Int64()))

			case Int96:
				i96 := v.Int96()
				ccw.distinctCount.Insert(i96[:])

			case Float:
				ccw.distinctCount.InsertHash(math.Float64bits(float64(v.Float())))

			case Double:
				ccw.distinctCount.InsertHash(math.Float64bits(v.Double()))

			case ByteArray, FixedLenByteArray:
				ccw.distinctCount.Insert(v.ByteArray())

			default: // null
				ccw.numNulls++
				ccw.statistics.NullCount++
			}

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
