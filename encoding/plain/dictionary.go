package plain

import (
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
)

type DictionaryEncoding struct {
	encoding.NotSupported
}

func (e *DictionaryEncoding) String() string {
	return "PLAIN_DICTIONARY"
}

func (e *DictionaryEncoding) Encoding() format.Encoding {
	return format.PlainDictionary
}

func (e *DictionaryEncoding) EncodeInt32(dst []byte, src []int32) ([]byte, error) {
	return append(dst[:0], bits.Int32ToBytes(src)...), nil
}

func (e *DictionaryEncoding) DecodeInt32(dst []int32, src []byte) ([]int32, error) {
	if (len(src) % 4) != 0 {
		return dst[:0], encoding.ErrInvalidInputSize(e, "INT32", len(src))
	}
	return append(dst[:0], bits.BytesToInt32(src)...), nil
}
