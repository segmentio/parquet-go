//go:build go1.18

package test

import (
	"fmt"
	"testing"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func EncodeInt32(t *testing.T, enc encoding.Encoding, min, max int, bitWidth uint) {
	t.Helper()
	encode(t, enc, min, max,
		encoding.Encoding.EncodeInt32,
		encoding.Encoding.DecodeInt32,
		func(i int) int32 {
			value := int32(i)
			mask := int32((1 << bitWidth) - 1)
			if (i % 2) != 0 {
				value = -value
			}
			return value & mask
		},
	)
}

func EncodeFloat(t *testing.T, enc encoding.Encoding, min, max int) {
	t.Helper()
	encode(t, enc, min, max,
		encoding.Encoding.EncodeFloat,
		encoding.Encoding.DecodeFloat,
		func(i int) float32 { return float32(i) },
	)
}

func EncodeDouble(t *testing.T, enc encoding.Encoding, min, max int) {
	t.Helper()
	encode(t, enc, min, max,
		encoding.Encoding.EncodeDouble,
		encoding.Encoding.DecodeDouble,
		func(i int) float64 { return float64(i) },
	)
}

type encodingMethod func(encoding.Encoding, []byte, []byte) ([]byte, error)

func encode[T comparable](t *testing.T, enc encoding.Encoding, min, max int, encode, decode encodingMethod, valueOf func(int) T) {
	t.Helper()

	for k := min; k <= max; k++ {
		t.Run(fmt.Sprintf("N=%d", k), func(t *testing.T) {
			src := make([]T, k)
			for i := range src {
				src[i] = valueOf(i)
			}

			buf, err := encode(enc, nil, unsafecast.Slice[byte](src))
			if err != nil {
				t.Fatalf("encoding %d values: %v", k, err)
			}

			res, err := decode(enc, nil, buf)
			if err != nil {
				t.Fatalf("decoding %d values: %v", k, err)
			}

			if err := assertEqual(src, unsafecast.Slice[T](res)); err != nil {
				t.Fatalf("testing %d values: %v", k, err)
			}
		})
	}
}

func assertEqual[T comparable](want, got []T) error {
	if len(want) != len(got) {
		return fmt.Errorf("number of values mismatch: want=%d got=%d", len(want), len(got))
	}

	for i := range want {
		if want[i] != got[i] {
			return fmt.Errorf("values at index %d/%d mismatch: want=%+v got=%+v", i, len(want), want[i], got[i])
		}
	}

	return nil
}
