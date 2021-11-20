package encoding

import (
	"io"

	"github.com/segmentio/parquet/format"
)

type Encoding interface {
	Encoding() format.Encoding
	CanEncode(format.Type) bool
	NewDecoder(io.Reader) Decoder
	NewEncoder(io.Writer) Encoder
}

type Encoder interface {
	io.Closer
	Reset(io.Writer)
	Encoding() format.Encoding
	EncodeBoolean(data []bool) error
	EncodeInt32(data []int32) error
	EncodeInt64(data []int64) error
	EncodeInt96(data [][12]byte) error
	EncodeFloat(data []float32) error
	EncodeDouble(data []float64) error
	EncodeByteArray(data [][]byte) error
	EncodeFixedLenByteArray(size int, data []byte) error
	EncodeIntArray(data IntArrayView) error
	SetBitWidth(bitWidth int)
}

type Decoder interface {
	io.Closer
	Reset(io.Reader)
	Encoding() format.Encoding
	DecodeBoolean(data []bool) (int, error)
	DecodeInt32(data []int32) (int, error)
	DecodeInt64(data []int64) (int, error)
	DecodeInt96(data [][12]byte) (int, error)
	DecodeFloat(data []float32) (int, error)
	DecodeDouble(data []float64) (int, error)
	DecodeByteArray(data [][]byte) (int, error)
	DecodeFixedLenByteArray(size int, data []byte) (int, error)
	DecodeIntArray(data IntArrayBuffer) error
	SetBitWidth(bitWidth int)
}
