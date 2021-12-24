package delta

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/format"
)

type ByteArrayEncoder struct {
	encoding.NotSupportedEncoder
	deltas   BinaryPackedEncoder
	arrays   LengthByteArrayEncoder
	prefixes []int32
}

func NewByteArrayEncoder(w io.Writer) *ByteArrayEncoder {
	e := &ByteArrayEncoder{prefixes: make([]int32, defaultBufferSize/4)}
	e.Reset(w)
	return e
}

func (e *ByteArrayEncoder) Reset(w io.Writer) {
	e.deltas.Reset(w)
	e.arrays.Reset(w)
	e.prefixes = e.prefixes[:0]
}

func (e *ByteArrayEncoder) Encoding() format.Encoding {
	return format.DeltaByteArray
}

func (e *ByteArrayEncoder) EncodeByteArray(data []byte) error {
	lastValue := ([]byte)(nil)
	suffixes := data[:0] // TODO: is it OK to reuse the input buffer?
	e.prefixes = e.prefixes[:0]

	_, err := plain.ScanByteArrayList(data, plain.All, func(value []byte) error {
		if len(value) > math.MaxInt32 {
			return fmt.Errorf("DELTA_BYTE_ARRAY: byte array of length %d is too large to be encoded", len(value))
		}
		n := prefixLength(lastValue, value)
		e.prefixes = append(e.prefixes, int32(n))
		lastValue = value
		suffixes = plain.AppendByteArray(suffixes, value[n:])
		return nil
	})
	if err != nil {
		return err
	}
	if err := e.deltas.EncodeInt32(e.prefixes); err != nil {
		return err
	}
	return e.arrays.EncodeByteArray(suffixes)
}

func prefixLength(base, data []byte) int {
	return binarySearchPrefixLength(len(base)/2, base, data)
}

func binarySearchPrefixLength(max int, base, data []byte) int {
	for len(base) > 0 {
		if bytes.HasPrefix(data, base[:max]) {
			if max == len(base) {
				return max
			}
			max += (len(base)-max)/2 + 1
		} else {
			base = base[:max-1]
			max /= 2
		}
	}
	return 0
}
