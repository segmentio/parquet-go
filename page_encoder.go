package parquet

import (
	"encoding/binary"
	"io"

	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/encoding/rle"
	"github.com/segmentio/parquet-go/internal/bits"
)

type PageEncoder interface {
	// Encodes an array of boolean values using this encoder.
	EncodeBoolean(data []bool) error

	// Encodes an array of 32 bit integer values using this encoder.
	EncodeInt32(data []int32) error

	// Encodes an array of 64 bit integer values using this encoder.
	EncodeInt64(data []int64) error

	// Encodes an array of 96 bit integer values using this encoder.
	EncodeInt96(data []deprecated.Int96) error

	// Encodes an array of 32 bit floating point values using this encoder.
	EncodeFloat(data []float32) error

	// Encodes an array of 64 bit floating point values using this encoder.
	EncodeDouble(data []float64) error

	// Encodes an array of variable length byte array values using this encoder.
	EncodeByteArray(data []byte) error

	// Encodes an array of fixed length byte array values using this encoder.
	//
	// The list is encoded contiguously in the `data` byte slice, in chunks of
	// `size` elements
	EncodeFixedLenByteArray(data []byte, size int) error
}

type dataPageEncoderV1 struct{ *bufferedPageEncoder }

type dataPageEncoderV2 struct{ *bufferedPageEncoder }

type dictionaryPageEncoder struct{ *bufferedPageEncoder }

type bufferedPageEncoder struct {
	levels struct {
		encoding    rle.Encoding
		repetitions []byte
		definitions []byte
	}
	page struct {
		encoding encoding.Encoding
		data     []byte
		size     int32
	}
	compression struct {
		codec  compress.Codec
		buffer []byte
	}
}

func (e *bufferedPageEncoder) writeDataPageV1(page BufferedPage, maxRepetitionLevel, maxDefinitionLevel int8) (err error) {
	if err = e.writeDataPage(page, maxRepetitionLevel, maxDefinitionLevel); err != nil {
		return err
	}
	e.compression.buffer = e.compression.buffer[:0]

	if len(e.levels.repetitions) > 0 {
		e.compression.buffer = appendInt32(e.compression.buffer, int32(len(e.levels.repetitions)))
		e.compression.buffer = append(e.compression.buffer, e.levels.repetitions...)
		e.levels.repetitions = e.levels.repetitions[:0]
	}

	if len(e.levels.definitions) > 0 {
		e.compression.buffer = appendInt32(e.compression.buffer, int32(len(e.levels.definitions)))
		e.compression.buffer = append(e.compression.buffer, e.levels.definitions...)
		e.levels.definitions = e.levels.definitions[:0]
	}

	e.compression.buffer = append(e.compression.buffer, e.page.data...)
	if isCompressed(e.compression.codec) {
		e.page.data, err = e.compression.codec.Encode(e.page.data, e.compression.buffer)
	} else {
		e.page.data, e.compression.buffer = e.compression.buffer, e.page.data
	}
	return err
}

func (e *bufferedPageEncoder) writeDataPageV2(page BufferedPage, maxRepetitionLevel, maxDefinitionLevel int8) (err error) {
	if err = e.writeDataPage(page, maxRepetitionLevel, maxDefinitionLevel); err != nil {
		return err
	}
	if isCompressed(e.compression.codec) && !isDictionaryEncoding(e.page.encoding) {
		e.compression.buffer, err = e.compression.codec.Encode(e.compression.buffer, e.page.data)
		e.page.data, e.compression.buffer = e.compression.buffer, e.page.data
	}
	return err
}

func (e *bufferedPageEncoder) writeDataPage(page BufferedPage, maxRepetitionLevel, maxDefinitionLevel int8) (err error) {
	e.levels.repetitions = e.levels.repetitions[:0]
	e.levels.definitions = e.levels.definitions[:0]
	e.page.data = e.page.data[:0]
	e.page.size = 0

	if maxRepetitionLevel > 0 {
		levels := page.RepetitionLevels()
		e.levels.encoding.BitWidth = bits.Len8(maxRepetitionLevel)
		e.levels.repetitions, err = e.levels.encoding.EncodeInt8(e.levels.repetitions, levels)
		if err != nil {
			return err
		}
	}

	if maxDefinitionLevel > 0 {
		levels := page.DefinitionLevels()
		e.levels.encoding.BitWidth = bits.Len8(maxDefinitionLevel)
		e.levels.definitions, err = e.levels.encoding.EncodeInt8(e.levels.definitions, levels)
		if err != nil {
			return err
		}
	}

	if err = page.WriteTo(legacyPageEncoder{e}); err != nil {
		return err
	}

	e.page.size = int32(len(e.page.data))
	return nil
}

func (e *bufferedPageEncoder) writeDictionaryPage(page BufferedPage) (err error) {
	if err = page.WriteTo(legacyPageEncoder{e}); err != nil {
		return err
	}
	if isCompressed(e.compression.codec) {
		e.compression.buffer, err = e.compression.codec.Encode(e.compression.buffer, e.page.data)
		e.page.data, e.compression.buffer = e.compression.buffer, e.page.data
	}
	return err
}

func (e *bufferedPageEncoder) EncodeBoolean(data []bool) (err error) {
	e.page.data, err = e.page.encoding.EncodeBoolean(e.page.data, data)
	return err
}

func (e *bufferedPageEncoder) EncodeInt32(data []int32) (err error) {
	e.page.data, err = e.page.encoding.EncodeInt32(e.page.data, data)
	return err
}

func (e *bufferedPageEncoder) EncodeInt64(data []int64) (err error) {
	e.page.data, err = e.page.encoding.EncodeInt64(e.page.data, data)
	return err
}

func (e *bufferedPageEncoder) EncodeInt96(data []deprecated.Int96) (err error) {
	e.page.data, err = e.page.encoding.EncodeInt96(e.page.data, data)
	return err
}

func (e *bufferedPageEncoder) EncodeFloat(data []float32) (err error) {
	e.page.data, err = e.page.encoding.EncodeFloat(e.page.data, data)
	return err
}

func (e *bufferedPageEncoder) EncodeDouble(data []float64) (err error) {
	e.page.data, err = e.page.encoding.EncodeDouble(e.page.data, data)
	return err
}

func (e *bufferedPageEncoder) EncodeByteArray(data []byte) (err error) {
	e.page.data, err = e.page.encoding.EncodeByteArray(e.page.data, data)
	return err
}

func (e *bufferedPageEncoder) EncodeFixedLenByteArray(data []byte, size int) (err error) {
	e.page.data, err = e.page.encoding.EncodeFixedLenByteArray(e.page.data, data, size)
	return err
}

type legacyPageEncoder struct {
	base *bufferedPageEncoder
}

func (e legacyPageEncoder) Reset(io.Writer) {
	panic("NOT IMPLEMENTED")
}

func (e legacyPageEncoder) EncodeBoolean(data []bool) error {
	return e.base.EncodeBoolean(data)
}

func (e legacyPageEncoder) EncodeInt8(data []int8) error {
	panic("NOT IMPLEMENTED")
}

func (e legacyPageEncoder) EncodeInt16(data []int16) error {
	panic("NOT IMPLEMENTED")
}

func (e legacyPageEncoder) EncodeInt32(data []int32) error {
	return e.base.EncodeInt32(data)
}

func (e legacyPageEncoder) EncodeInt64(data []int64) error {
	return e.base.EncodeInt64(data)
}

func (e legacyPageEncoder) EncodeInt96(data []deprecated.Int96) error {
	return e.base.EncodeInt96(data)
}

func (e legacyPageEncoder) EncodeFloat(data []float32) error {
	return e.base.EncodeFloat(data)
}

func (e legacyPageEncoder) EncodeDouble(data []float64) error {
	return e.base.EncodeDouble(data)
}

func (e legacyPageEncoder) EncodeByteArray(data encoding.ByteArrayList) error {
	values := make([]byte, 0, data.Size())
	data.Range(func(value []byte) bool {
		values = plain.AppendByteArray(values, value)
		return true
	})
	return e.base.EncodeByteArray(values)
}

func (e legacyPageEncoder) EncodeFixedLenByteArray(size int, data []byte) error {
	return e.base.EncodeFixedLenByteArray(data, size)
}

func (e legacyPageEncoder) SetBitWidth(bitWidth int) {
	panic("NOT IMPLEMENTED")
}

func appendInt32(b []byte, v int32) []byte {
	c := [4]byte{}
	binary.LittleEndian.PutUint32(c[:], uint32(v))
	return append(b, c[:]...)
}
