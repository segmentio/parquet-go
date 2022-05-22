package test

import (
	"testing"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func EncodeFloat(t *testing.T, e encoding.Encoding, n int) {
	t.Helper()
	encode(t, e, n,
		encoding.Encoding.EncodeFloat,
		encoding.Encoding.DecodeFloat,
		func(i int) float32 { return float32(i) },
	)
}

type encodingMethod func(encoding.Encoding, []byte, []byte) ([]byte, error)

func encode[T comparable](t *testing.T, e encoding.Encoding, n int, encode, decode encodingMethod, valueOf func(int) T) {
	t.Helper()

	src := make([]T, n)
	for i := range src {
		src[i] = valueOf(i)
	}

	buf, err := encode(e, nil, unsafecast.Slice[byte](src))
	if err != nil {
		t.Fatal("encoding:", err)
	}

	res, err := decode(e, nil, buf)
	if err != nil {
		t.Fatal("decoding:", err)
	}

	assertEqual(t, src, unsafecast.Slice[T](res))
}

func assertEqual[T comparable](t *testing.T, want, got []T) {
	t.Helper()

	if len(want) != len(got) {
		t.Errorf("number of values mismatch: want=%d got=%d", len(want), len(got))
	} else {
		for i := range want {
			if want[i] != got[i] {
				t.Errorf("values at index %d/%d mismatch: want=%+v got=%+v", i, len(want), want[i], got[i])
			}
		}
	}
}
