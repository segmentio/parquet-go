//go:build go1.18
// +build go1.18

// Package fuzz contains functions to help fuzz test parquet encodings.
package fuzz

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func EncodeBoolean(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeBoolean,
		encoding.Encoding.DecodeBoolean,
	)
}

func EncodeInt32(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeInt32,
		encoding.Encoding.DecodeInt32,
	)
}

func EncodeInt64(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeInt64,
		encoding.Encoding.DecodeInt64,
	)
}

func EncodeFloat32(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeFloat,
		encoding.Encoding.DecodeFloat,
	)
}

func EncodeFloat64(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeDouble,
		encoding.Encoding.DecodeDouble,
	)
}

func EncodeByteArray(f *testing.F, e encoding.Encoding) {
	var err error
	var buf = make([]byte, 64*1024)
	var dst = make([]byte, 64*1024)
	var src = make([]byte, 64*1024)
	var prng = rand.New(rand.NewSource(0))

	f.Fuzz(func(t *testing.T, input []byte, seed int64) {
		prng.Seed(seed)
		src = generatePlainByteArrayList(src[:0], input, prng)

		dst, err = e.EncodeByteArray(dst, src)
		if err != nil {
			t.Error(err)
			return
		}
		buf, err = e.DecodeByteArray(buf, dst)
		if err != nil {
			t.Error(err)
			return
		}
		if !bytes.Equal(buf, src) {
			t.Error("decoded output does not match the original input")
			return
		}
		// Likely invalid inputs, look for panics.
		buf, _ = e.DecodeByteArray(buf, input)
	})
}

func encode[T bool | int32 | int64 | float32 | float64](f *testing.F, e encoding.Encoding, encode func(encoding.Encoding, []byte, []T) ([]byte, error), decode func(encoding.Encoding, []T, []byte) ([]T, error)) {
	var err error
	var buf = make([]T, 16*1024)
	var dst = make([]byte, 64*1024)

	f.Fuzz(func(t *testing.T, input []byte) {
		var src = unsafecast.Slice[T](input)
		dst, err = encode(e, dst, src)
		if err != nil {
			t.Error(err)
			return
		}
		buf, err = decode(e, buf, dst)
		if err != nil {
			t.Error(err)
			return
		}
		if !bytes.Equal(unsafecast.Slice[byte](buf), unsafecast.Slice[byte](src)) {
			t.Error("decoded output does not match the original input")
			return
		}
		// Likely invalid inputs, look for panics.
		buf, _ = decode(e, buf, input)
	})
}

func generatePlainByteArrayList(dst, src []byte, prng *rand.Rand) []byte {
	limit := len(src)/10 + 1

	for i := 0; i < len(src); {
		n := prng.Intn(limit) + 1
		r := len(src) - i
		if n > r {
			n = r
		}
		dst = plain.AppendByteArray(dst, src[i:i+n])
		i += n
	}

	return dst
}
