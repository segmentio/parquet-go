package encoding

import (
	"io"
)

type Encoding interface {
	NewBooleanDecoder(io.Reader) BooleanDecoder
	NewBooleanEncoder(io.Writer) BooleanEncoder

	NewInt32Decoder(io.Reader) Int32Decoder
	NewInt32Encoder(io.Writer) Int32Encoder

	NewInt64Decoder(io.Reader) Int64Decoder
	NewInt64Encoder(io.Writer) Int64Encoder

	NewInt96Decoder(io.Reader) Int96Decoder
	NewInt96Encoder(io.Writer) Int96Encoder

	NewFloatDecoder(io.Reader) FloatDecoder
	NewFloatEncoder(io.Writer) FloatEncoder

	NewDoubleDecoder(io.Reader) DoubleDecoder
	NewDoubleEncoder(io.Writer) DoubleEncoder

	NewByteArrayDecoder(io.Reader) ByteArrayDecoder
	NewByteArrayEncoder(io.Writer) ByteArrayEncoder

	NewFixedLenByteArrayDecoder(io.Reader) FixedLenByteArrayDecoder
	NewFixedLenByteArrayEncoder(io.Writer) FixedLenByteArrayEncoder
}

type Decoder interface {
	io.Closer
	Reset(io.Reader)
}

type Encoder interface {
	io.Closer
	Reset(io.Writer)
}

type BooleanDecoder interface {
	Decoder
	DecodeBoolean(data []bool) (int, error)
}

type BooleanEncoder interface {
	Encoder
	EncodeBoolean(data []bool) error
}

type IntDecoder interface {
	Decoder
	SetBitWidth(bitWidth int)
}

type IntEncoder interface {
	Encoder
	SetBitWidth(bitWidth int)
}

type Int32Decoder interface {
	IntDecoder
	DecodeInt32(data []int32) (int, error)
}

type Int32Encoder interface {
	IntEncoder
	EncodeInt32(data []int32) error
}

type Int64Decoder interface {
	IntDecoder
	DecodeInt64(data []int64) (int, error)
}

type Int64Encoder interface {
	IntEncoder
	EncodeInt64(data []int64) error
}

type Int96Decoder interface {
	IntDecoder
	DecodeInt96(data [][12]byte) (int, error)
}

type Int96Encoder interface {
	IntEncoder
	EncodeInt96(data [][12]byte) error
}

type FloatDecoder interface {
	Decoder
	DecodeFloat(data []float32) (int, error)
}

type FloatEncoder interface {
	Encoder
	EncodeFloat(data []float32) error
}

type DoubleDecoder interface {
	Decoder
	DecodeDouble(data []float64) (int, error)
}

type DoubleEncoder interface {
	Encoder
	EncodeDouble(data []float64) error
}

type ByteArrayDecoder interface {
	Decoder
	DecodeByteArray(data [][]byte) (int, error)
}

type ByteArrayEncoder interface {
	Encoder
	EncodeByteArray(data [][]byte) error
}

type FixedLenByteArrayDecoder interface {
	Decoder
	DecodeFixedLenByteArray(size int, data []byte) (int, error)
}

type FixedLenByteArrayEncoder interface {
	Encoder
	EncodeFixedLenByteArray(size int, data []byte) error
}
