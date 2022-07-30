package rle

import (
	"math/bits"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

type DictionaryEncoding struct {
	encoding.NotSupported
}

func (e *DictionaryEncoding) String() string {
	return "RLE_DICTIONARY"
}

func (e *DictionaryEncoding) Encoding() format.Encoding {
	return format.RLEDictionary
}

func (e *DictionaryEncoding) EncodeInt32(dst []byte, src encoding.Values) ([]byte, error) {
	values := src.Int32()
	bitWidth := maxLenInt32(values)
	dst = append(dst[:0], byte(bitWidth))
	dst, err := encodeInt32(dst, values, uint(bitWidth))
	return dst, e.wrap(err)
}

func (e *DictionaryEncoding) DecodeInt32(dst encoding.Values, src []byte) (encoding.Values, error) {
	values := dst.Bytes(encoding.Int32)[:0]
	if len(src) == 0 {
		return encoding.Int32ValuesFromBytes(values), nil
	}
	buf, err := decodeInt32(values, src[1:], uint(src[0]))
	return encoding.Int32ValuesFromBytes(buf), e.wrap(err)
}

func (e *DictionaryEncoding) wrap(err error) error {
	if err != nil {
		err = encoding.Error(e, err)
	}
	return err
}

func clearInt32(data []int32) {
	for i := range data {
		data[i] = 0
	}
}

func maxLenInt32(data []int32) (max int) {
	for _, v := range data {
		if n := bits.Len32(uint32(v)); n > max {
			max = n
		}
	}
	return max
}
