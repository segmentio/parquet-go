package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type WriterConfig struct {
	PageBufferSize int

	ColumnsConfig map[string]*WriterConfig
}

func (cfg *WriterConfig) Apply(options ...WriterOption) {
	for _, opt := range options {
		opt.Configure(cfg)
	}
}

func (cfg *WriterConfig) Configure(config *WriterConfig) { *config = *cfg }

func (cfg *WriterConfig) ColumnConfig(column string) *WriterConfig {
	if columnConfig := cfg.ColumnsConfig[column]; columnConfig != nil {
		return columnConfig
	} else {
		columnConfig = new(WriterConfig)
		cfg.Configure(columnConfig)
		columnConfig.ColumnsConfig = nil
		return columnConfig
	}
}

type WriterOption interface {
	Configure(*WriterConfig)
}

type writerOption func(*WriterConfig)

func (opt writerOption) Configure(config *WriterConfig) { opt(config) }

func PageBufferSize(size int) WriterOption {
	return writerOption(func(config *WriterConfig) { config.PageBufferSize = size })
}

func ColumnConfig(column string, options ...WriterOption) WriterOption {
	return writerOption(func(config *WriterConfig) {
		columnConfig := *config
		columnConfig.Apply(options...)

		if config.ColumnsConfig == nil {
			config.ColumnsConfig = make(map[string]*WriterConfig)
		}

		config.ColumnsConfig[column] = &columnConfig
	})
}

type RowGroupWriter struct {
	once    sync.Once
	writer  io.Writer
	schema  Node
	object  Object
	row     Row
	columns []*rowGroupColumn
}

type rowGroupColumn struct {
	path   []string
	buffer *bytes.Buffer
	writer *ColumnChunkWriter

	maxDefinitionLevel int32
	maxRepetitionLevel int32
}

func NewRowGroupWriter(writer io.Writer, schema Node, options ...WriterOption) *RowGroupWriter {
	rgw := &RowGroupWriter{
		writer: writer,
		schema: schema,
		object: schema.Object(reflect.Value{}),
	}
	config := &WriterConfig{}
	config.Apply(options...)
	rgw.init(schema, nil, 0, 0, config)
	return rgw
}

func (rgw *RowGroupWriter) init(node Node, path []string, maxRepetitionLevel, maxDefinitionLevel int32, config *WriterConfig) {
	if !node.Required() {
		maxDefinitionLevel++
	}
	if node.Repeated() {
		maxRepetitionLevel++
	}

	if names := node.ChildNames(); len(names) > 0 {
		base := path[:len(path):len(path)]

		for _, name := range names {
			rgw.init(node.ChildByName(name), append(base, name), maxRepetitionLevel, maxDefinitionLevel, config.ColumnConfig(name))
		}
	} else {
		buffer := new(bytes.Buffer)

		rgw.columns = append(rgw.columns, &rowGroupColumn{
			path:               path,
			buffer:             buffer,
			writer:             NewColumnChunkWriter(buffer, &Uncompressed, &Plain, node.Type().NewPageBuffer(config.PageBufferSize)),
			maxRepetitionLevel: maxRepetitionLevel,
			maxDefinitionLevel: maxDefinitionLevel,
		})
	}
}

func (rgw *RowGroupWriter) Flush() error {
	return nil
}

func (rgw *RowGroupWriter) WriteRow(row interface{}) error {
	rgw.object.Reset(reflect.ValueOf(row))
	rgw.row.Reset(rgw.object)

	for rgw.row.Next() {
		rgw.columns[rgw.row.ColumnIndex()].writer.WriteValue(rgw.row.Value())
	}

	return nil
}

type ColumnChunkWriter struct {
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

	numValues    int32
	numNulls     int32
	numRows      int32
	isCompressed bool
}

func NewColumnChunkWriter(writer io.Writer, compression compress.Codec, encoding encoding.Encoding, values PageBuffer) *ColumnChunkWriter {
	return &ColumnChunkWriter{
		writer:       writer,
		encoding:     encoding,
		compression:  compression,
		values:       values,
		isCompressed: true,
	}
}

func (ccw *ColumnChunkWriter) Flush() error {
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

	if err := ccw.values.WriteTo(ccw.page.encoder); err != nil {
		return err
	}

	minValue, maxValue := ccw.values.Bounds()
	ccw.header.buffer.Reset()
	ccw.header.encoder.Reset(ccw.header.protocol.NewWriter(&ccw.header.buffer))

	if err := ccw.header.encoder.Encode(&format.PageHeader{
		Type:                 format.DataPageV2,
		UncompressedPageSize: int32(ccw.page.uncompressed.length),
		CompressedPageSize:   int32(ccw.page.buffer.Len()),
		CRC:                  int32(ccw.page.checksum.Sum32()),
		DataPageHeaderV2: &format.DataPageHeaderV2{
			NumValues: ccw.numValues,
			NumNulls:  ccw.numNulls,
			NumRows:   ccw.numRows,
			Encoding:  ccw.encoding.Encoding(),
			// DefinitionLevelsByteLength:
			// RepetitionLevelsByteLength:
			IsCompressed: &ccw.isCompressed,
			Statistics: format.Statistics{
				Min:      minValue, // deprecated
				Max:      maxValue, // deprecated
				MinValue: minValue,
				MaxValue: maxValue,
			},
		},
	}); err != nil {
		return err
	}
	if _, err := ccw.header.buffer.WriteTo(ccw.writer); err != nil {
		return err
	}
	if _, err := ccw.page.buffer.WriteTo(ccw.writer); err != nil {
		return err
	}

	ccw.values.Reset()
	ccw.numValues = 0
	ccw.numNulls = 0
	ccw.numRows = 0
	return nil
}

func (ccw *ColumnChunkWriter) WriteValue(v Value) error {
	for {
		switch err := ccw.values.WriteValue(v); err {
		case nil:
			ccw.numValues++
			ccw.numRows++
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
