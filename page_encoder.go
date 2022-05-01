package parquet

import (
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
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

type bufferedPageEncoder struct {
	encoding encoding.Encoding
	buffer   []byte
}

func (e *bufferedPageEncoder) EncodeBoolean(data []bool) (err error) {
	e.buffer, err = e.encoding.EncodeBoolean(e.buffer, data)
	return err
}

func (e *bufferedPageEncoder) EncodeInt32(data []int32) (err error) {
	e.buffer, err = e.encoding.EncodeInt32(e.buffer, data)
	return err
}

func (e *bufferedPageEncoder) EncodeInt64(data []int64) (err error) {
	e.buffer, err = e.encoding.EncodeInt64(e.buffer, data)
	return err
}

func (e *bufferedPageEncoder) EncodeInt96(data []deprecated.Int96) (err error) {
	e.buffer, err = e.encoding.EncodeInt96(e.buffer, data)
	return err
}

func (e *bufferedPageEncoder) EncodeFloat(data []float32) (err error) {
	e.buffer, err = e.encoding.EncodeFloat(e.buffer, data)
	return err
}

func (e *bufferedPageEncoder) EncodeDouble(data []float64) (err error) {
	e.buffer, err = e.encoding.EncodeDouble(e.buffer, data)
	return err
}

func (e *bufferedPageEncoder) EncodeByteArray(data []byte) (err error) {
	e.buffer, err = e.encoding.EncodeByteArray(e.buffer, data)
	return err
}

func (e *bufferedPageEncoder) EncodeFixedLenByteArray(data []byte, size int) (err error) {
	e.buffer, err = e.encoding.EncodeFixedLenByteArray(e.buffer, data, size)
	return err
}
