package plain

import (
	"io"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

type DictionaryEncoding struct {
	base Encoding
}

func (e *DictionaryEncoding) Encoding() format.Encoding {
	return format.PlainDictionary
}

func (e *DictionaryEncoding) CanEncode(t format.Type) bool {
	return true
}

func (e *DictionaryEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewDecoder(r)
}

func (e *DictionaryEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoder(w)
}

func (e *DictionaryEncoding) String() string {
	return "PLAIN_DICTIONARY"
}

func (e *DictionaryEncoding) EncodeBoolean(dst []byte, src []bool) ([]byte, error) {
	return e.base.EncodeBoolean(dst, src)
}

func (e *DictionaryEncoding) EncodeInt8(dst []byte, src []int8) ([]byte, error) {
	return e.base.EncodeInt8(dst, src)
}

func (e *DictionaryEncoding) EncodeInt32(dst []byte, src []int32) ([]byte, error) {
	return e.base.EncodeInt32(dst, src)
}

func (e *DictionaryEncoding) EncodeInt64(dst []byte, src []int64) ([]byte, error) {
	return e.base.EncodeInt64(dst, src)
}

func (e *DictionaryEncoding) EncodeInt96(dst []byte, src []deprecated.Int96) ([]byte, error) {
	return e.base.EncodeInt96(dst, src)
}

func (e *DictionaryEncoding) EncodeFloat(dst []byte, src []float32) ([]byte, error) {
	return e.base.EncodeFloat(dst, src)
}

func (e *DictionaryEncoding) EncodeDouble(dst []byte, src []float64) ([]byte, error) {
	return e.base.EncodeDouble(dst, src)
}

func (e *DictionaryEncoding) EncodeByteArray(dst []byte, src []byte) ([]byte, error) {
	return e.base.EncodeByteArray(dst, src)
}

func (e *DictionaryEncoding) EncodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error) {
	return e.base.EncodeFixedLenByteArray(dst, src, size)
}

func (e *DictionaryEncoding) DecodeBoolean(dst []bool, src []byte) ([]bool, error) {
	return e.base.DecodeBoolean(dst, src)
}

func (e *DictionaryEncoding) DecodeInt8(dst []int8, src []byte) ([]int8, error) {
	return e.base.DecodeInt8(dst, src)
}

func (e *DictionaryEncoding) DecodeInt32(dst []int32, src []byte) ([]int32, error) {
	return e.base.DecodeInt32(dst, src)
}

func (e *DictionaryEncoding) DecodeInt64(dst []int64, src []byte) ([]int64, error) {
	return e.base.DecodeInt64(dst, src)
}

func (e *DictionaryEncoding) DecodeInt96(dst []deprecated.Int96, src []byte) ([]deprecated.Int96, error) {
	return e.base.DecodeInt96(dst, src)
}

func (e *DictionaryEncoding) DecodeFloat(dst []float32, src []byte) ([]float32, error) {
	return e.base.DecodeFloat(dst, src)
}

func (e *DictionaryEncoding) DecodeDouble(dst []float64, src []byte) ([]float64, error) {
	return e.base.DecodeDouble(dst, src)
}

func (e *DictionaryEncoding) DecodeByteArray(dst, src []byte) ([]byte, error) {
	return e.base.DecodeByteArray(dst, src)
}

func (e *DictionaryEncoding) DecodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error) {
	return e.base.DecodeFixedLenByteArray(dst, src, size)
}
