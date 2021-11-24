package encoding

import (
	"errors"
	"fmt"
	"io"

	"github.com/segmentio/parquet/format"
)

var (
	ErrNotImplemented = errors.New("encoding not implemented")
)

type NotImplemented struct {
}

func (NotImplemented) Encoding() format.Encoding {
	return -1
}

func (NotImplemented) CanEncode(format.Type) bool {
	return false
}

func (NotImplemented) NewDecoder(io.Reader) Decoder {
	return NotImplementedDecoder{}
}

func (NotImplemented) NewEncoder(io.Writer) Encoder {
	return NotImplementedEncoder{}
}

func (NotImplemented) String() string {
	return "NOT_IMPLEMENTED"
}

type NotImplementedDecoder struct {
}

func (NotImplementedDecoder) Encoding() format.Encoding {
	return -1
}

func (NotImplementedDecoder) Reset(io.Reader) {
}

func (NotImplementedDecoder) DecodeBoolean([]bool) (int, error) {
	return 0, NotImplementedError("BOOLEAN")
}

func (NotImplementedDecoder) DecodeInt8([]int8) (int, error) {
	return 0, NotImplementedError("INT8")
}

func (NotImplementedDecoder) DecodeInt16([]int16) (int, error) {
	return 0, NotImplementedError("INT16")
}

func (NotImplementedDecoder) DecodeInt32([]int32) (int, error) {
	return 0, NotImplementedError("INT32")
}

func (NotImplementedDecoder) DecodeInt64([]int64) (int, error) {
	return 0, NotImplementedError("INT64")
}

func (NotImplementedDecoder) DecodeInt96([][12]byte) (int, error) {
	return 0, NotImplementedError("INT96")
}

func (NotImplementedDecoder) DecodeFloat([]float32) (int, error) {
	return 0, NotImplementedError("FLOAT")
}

func (NotImplementedDecoder) DecodeDouble([]float64) (int, error) {
	return 0, NotImplementedError("DOUBLE")
}

func (NotImplementedDecoder) DecodeByteArray([]byte) (int, error) {
	return 0, NotImplementedError("BYTE_ARRAY")
}

func (NotImplementedDecoder) DecodeFixedLenByteArray(size int, data []byte) (int, error) {
	return 0, NotImplementedError("FIXED_LEN_BYTE_ARRAY")
}

func (NotImplementedDecoder) SetBitWidth(int) {
}

type NotImplementedEncoder struct {
}

func (NotImplementedEncoder) Encoding() format.Encoding {
	return -1
}

func (NotImplementedEncoder) Flush() error {
	return nil
}

func (NotImplementedEncoder) Reset(io.Writer) {
}

func (NotImplementedEncoder) EncodeBoolean([]bool) error {
	return NotImplementedError("BOOLEAN")
}

func (NotImplementedEncoder) EncodeInt8([]int8) error {
	return NotImplementedError("INT8")
}

func (NotImplementedEncoder) EncodeInt16([]int16) error {
	return NotImplementedError("INT16")
}

func (NotImplementedEncoder) EncodeInt32([]int32) error {
	return NotImplementedError("INT32")
}

func (NotImplementedEncoder) EncodeInt64([]int64) error {
	return NotImplementedError("INT64")
}

func (NotImplementedEncoder) EncodeInt96([][12]byte) error {
	return NotImplementedError("INT96")
}

func (NotImplementedEncoder) EncodeFloat([]float32) error {
	return NotImplementedError("FLOAT")
}

func (NotImplementedEncoder) EncodeDouble([]float64) error {
	return NotImplementedError("DOUBLE")
}

func (NotImplementedEncoder) EncodeByteArray([]byte) error {
	return NotImplementedError("BYTE_ARRAY")
}

func (NotImplementedEncoder) EncodeFixedLenByteArray(int, []byte) error {
	return NotImplementedError("FIXED_LEN_BYTE_ARRAY")
}

func (NotImplementedEncoder) SetBitWidth(int) {
}

func NotImplementedError(typ string) error {
	return fmt.Errorf("%w for type %s", ErrNotImplemented, typ)
}
