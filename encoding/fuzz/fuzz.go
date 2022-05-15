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
)

func EncodeBoolean(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeBoolean,
		encoding.Encoding.DecodeBoolean,
		generatePlainBooleanList,
	)
}

func EncodeLevels(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeLevels,
		encoding.Encoding.DecodeLevels,
		generatePlainValueList(1),
	)
}

func EncodeInt32(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeInt32,
		encoding.Encoding.DecodeInt32,
		generatePlainValueList(4),
	)
}

func EncodeInt64(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeInt64,
		encoding.Encoding.DecodeInt64,
		generatePlainValueList(8),
	)
}

func EncodeFloat(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeFloat,
		encoding.Encoding.DecodeFloat,
		generatePlainValueList(4),
	)
}

func EncodeDouble(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeDouble,
		encoding.Encoding.DecodeDouble,
		generatePlainValueList(8),
	)
}

func EncodeByteArray(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeByteArray,
		encoding.Encoding.DecodeByteArray,
		generatePlainByteArrayList,
	)
}

type encodingFunc func(enc encoding.Encoding, dst, src []byte) ([]byte, error)

type generateFunc func(dst, src []byte, prng *rand.Rand) []byte

func encode(f *testing.F, e encoding.Encoding, encode encodingFunc, decode encodingFunc, generate generateFunc) {
	const bufferSize = 64 * 1024
	var err error
	var buf = make([]byte, bufferSize)
	var src = make([]byte, bufferSize)
	var dst = make([]byte, bufferSize)
	var prng = rand.New(rand.NewSource(0))

	f.Fuzz(func(t *testing.T, input []byte, seed int64) {
		prng.Seed(seed)
		src = generate(src[:0], input, prng)

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

		if !bytes.Equal(buf, src) {
			t.Error("decoded output does not match the original input")
			return
		}

		// Likely invalid inputs, look for panics.
		buf, _ = decode(e, buf, input)
	})
}

func generatePlainBooleanList(dst, src []byte, _ *rand.Rand) []byte {
	for _, c := range src {
		dst = append(dst, (c & 1))
	}
	return dst
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

func generatePlainValueList(size int) func(dst, src []byte, _ *rand.Rand) []byte {
	return func(dst, src []byte, _ *rand.Rand) []byte {
		n := (len(src) / size) * size
		return append(dst, src[:n]...)
	}
}
