package parquet

import (
	"bytes"
	"fmt"
	"io"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/compress"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

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

	if ccw.page.compressed == nil {
		w, err := ccw.compression.NewWriter(&ccw.page.buffer)
		if err != nil {
			return fmt.Errorf("creating compressor for parquet column chunk writer: %w", err)
		}
		ccw.page.compressed = w
	} else {
		if err := ccw.page.compressed.Reset(&ccw.page.buffer); err != nil {
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
