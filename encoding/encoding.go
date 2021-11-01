package encoding

import (
	"io"
)

type Encoding interface {
	NewDecoder(io.Reader) Decoder
	NewEncoder(io.Writer) Encoder
}

type Decoder interface {
	Reset(io.Reader)
	DecodeBoolean(data []bool) (int, error)
	DecodeInt32(data []int32) (int, error)
	DecodeInt64(data []int64) (int, error)
	DecodeInt96(data [][12]byte) (int, error)
	DecodeFloat(data []float32) (int, error)
	DecodeDouble(data []float64) (int, error)
	DecodeByteArray(data [][]byte) (int, error)
	DecodeFixedLenByteArray(size int, data []byte) (int, error)
}

type Encoder interface {
	Reset(io.Writer)
	EncodeBoolean(data []bool) error
	EncodeInt32(data []int32) error
	EncodeInt64(data []int64) error
	EncodeInt96(data [][12]byte) error
	EncodeFloat(data []float32) error
	EncodeDouble(data []float64) error
	EncodeByteArray(data [][]byte) error
	EncodeFixedLenByteArray(size int, data []byte) error
}
