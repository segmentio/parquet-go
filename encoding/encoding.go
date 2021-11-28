package encoding

import (
	"errors"
	"fmt"
	"io"

	"github.com/segmentio/parquet/format"
)

var (
	ErrValueTooLarge  = errors.New("value is too large to be written to the buffer")
	ErrBufferTooShort = errors.New("buffer is too short to contain a single vlaue")
)

type Encoding interface {
	fmt.Stringer
	Encoding() format.Encoding
	CanEncode(format.Type) bool
	NewDecoder(io.Reader) Decoder
	NewEncoder(io.Writer) Encoder
}

type Encoder interface {
	Reset(io.Writer)
	Encoding() format.Encoding
	EncodeBoolean(data []bool) error
	EncodeInt8(data []int8) error
	EncodeInt16(data []int16) error
	EncodeInt32(data []int32) error
	EncodeInt64(data []int64) error
	EncodeInt96(data [][12]byte) error
	EncodeFloat(data []float32) error
	EncodeDouble(data []float64) error
	EncodeByteArray(data []byte) error
	EncodeFixedLenByteArray(size int, data []byte) error
	SetBitWidth(bitWidth int)
}

type Decoder interface {
	Reset(io.Reader)
	Encoding() format.Encoding
	DecodeBoolean(data []bool) (int, error)
	DecodeInt8(data []int8) (int, error)
	DecodeInt16(data []int16) (int, error)
	DecodeInt32(data []int32) (int, error)
	DecodeInt64(data []int64) (int, error)
	DecodeInt96(data [][12]byte) (int, error)
	DecodeFloat(data []float32) (int, error)
	DecodeDouble(data []float64) (int, error)
	DecodeByteArray(data []byte) (int, error)
	DecodeFixedLenByteArray(size int, data []byte) (int, error)
	SetBitWidth(bitWidth int)
}
