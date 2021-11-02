package encoding

import (
	"errors"
	"fmt"
	"io"
)

var (
	ErrNotImplemented = errors.New("encoding not implemented")
)

type NotImplemented struct{}

func (NotImplemented) NewBooleanDecoder(io.Reader) BooleanDecoder {
	return notImplementedDecoder{}
}

func (NotImplemented) NewBooleanEncoder(io.Writer) BooleanEncoder {
	return notImplementedEncoder{}
}

func (NotImplemented) NewInt32Decoder(io.Reader) Int32Decoder {
	return notImplementedDecoder{}
}

func (NotImplemented) NewInt32Encoder(io.Writer) Int32Encoder {
	return notImplementedEncoder{}
}

func (NotImplemented) NewInt64Decoder(io.Reader) Int64Decoder {
	return notImplementedDecoder{}
}

func (NotImplemented) NewInt64Encoder(io.Writer) Int64Encoder {
	return notImplementedEncoder{}
}

func (NotImplemented) NewInt96Decoder(io.Reader) Int96Decoder {
	return notImplementedDecoder{}
}

func (NotImplemented) NewInt96Encoder(io.Writer) Int96Encoder {
	return notImplementedEncoder{}
}

func (NotImplemented) NewFloatDecoder(io.Reader) FloatDecoder {
	return notImplementedDecoder{}
}

func (NotImplemented) NewFloatEncoder(io.Writer) FloatEncoder {
	return notImplementedEncoder{}
}

func (NotImplemented) NewDoubleDecoder(io.Reader) DoubleDecoder {
	return notImplementedDecoder{}
}

func (NotImplemented) NewDoubleEncoder(io.Writer) DoubleEncoder {
	return notImplementedEncoder{}
}

func (NotImplemented) NewByteArrayDecoder(io.Reader) ByteArrayDecoder {
	return notImplementedDecoder{}
}

func (NotImplemented) NewByteArrayEncoder(io.Writer) ByteArrayEncoder {
	return notImplementedEncoder{}
}

func (NotImplemented) NewFixedLenByteArrayDecoder(io.Reader) FixedLenByteArrayDecoder {
	return notImplementedDecoder{}
}

func (NotImplemented) NewFixedLenByteArrayEncoder(io.Writer) FixedLenByteArrayEncoder {
	return notImplementedEncoder{}
}

type notImplementedDecoder struct{}

func (notImplementedDecoder) Close() error {
	return nil
}

func (notImplementedDecoder) Reset(io.Reader) {
}

func (notImplementedDecoder) DecodeBoolean([]bool) (int, error) {
	return 0, NotImplementedError("BOOLEAN")
}

func (notImplementedDecoder) DecodeInt32([]int32) (int, error) {
	return 0, NotImplementedError("INT32")
}

func (notImplementedDecoder) DecodeInt64([]int64) (int, error) {
	return 0, NotImplementedError("INT64")
}

func (notImplementedDecoder) DecodeInt96([][12]byte) (int, error) {
	return 0, NotImplementedError("INT96")
}

func (notImplementedDecoder) DecodeFloat([]float32) (int, error) {
	return 0, NotImplementedError("FLOAT")
}

func (notImplementedDecoder) DecodeDouble([]float64) (int, error) {
	return 0, NotImplementedError("DOUBLE")
}

func (notImplementedDecoder) DecodeByteArray([][]byte) (int, error) {
	return 0, NotImplementedError("BYTE_ARRAY")
}

func (notImplementedDecoder) DecodeFixedLenByteArray(size int, data []byte) (int, error) {
	return 0, NotImplementedError("FIXED_LEN_BYTE_ARRAY")
}

func (notImplementedDecoder) SetBitWidth(int) {
}

type notImplementedEncoder struct{}

func (notImplementedEncoder) Close() error {
	return nil
}

func (notImplementedEncoder) Reset(io.Writer) {
}

func (notImplementedEncoder) EncodeBoolean([]bool) error {
	return NotImplementedError("BOOLEAN")
}

func (notImplementedEncoder) EncodeInt32([]int32) error {
	return NotImplementedError("INT32")
}

func (notImplementedEncoder) EncodeInt64([]int64) error {
	return NotImplementedError("INT64")
}

func (notImplementedEncoder) EncodeInt96([][12]byte) error {
	return NotImplementedError("INT96")
}

func (notImplementedEncoder) EncodeFloat([]float32) error {
	return NotImplementedError("FLOAT")
}

func (notImplementedEncoder) EncodeDouble([]float64) error {
	return NotImplementedError("DOUBLE")
}

func (notImplementedEncoder) EncodeByteArray([][]byte) error {
	return NotImplementedError("BYTE_ARRAY")
}

func (notImplementedEncoder) EncodeFixedLenByteArray(int, []byte) error {
	return NotImplementedError("FIXED_LEN_BYTE_ARRAY")
}

func (notImplementedEncoder) SetBitWidth(int) {
}

func NotImplementedError(typ string) error {
	return fmt.Errorf("%w for type %s", ErrNotImplemented, typ)
}
