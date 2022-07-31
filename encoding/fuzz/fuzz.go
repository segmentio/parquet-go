//go:build go1.18
// +build go1.18

// Package fuzz contains functions to help fuzz test parquet encodings.
package fuzz

import (
	"testing"
	"unsafe"

	"github.com/segmentio/parquet-go/encoding"
)

func EncodeBoolean(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeBoolean,
		encoding.Encoding.DecodeBoolean,
	)
}

func EncodeLevels(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeLevels,
		encoding.Encoding.DecodeLevels,
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

func EncodeFloat(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeFloat,
		encoding.Encoding.DecodeFloat,
	)
}

func EncodeDouble(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		encoding.Encoding.EncodeDouble,
		encoding.Encoding.DecodeDouble,
	)
}

func EncodeByteArray(f *testing.F, e encoding.Encoding) {
	encode(f, e,
		func(enc encoding.Encoding, dst []byte, src []string) ([]byte, error) {
			size := 0
			for _, s := range src {
				size += len(s)
			}

			offsets := make([]uint32, 0, len(src)+1)
			values := make([]byte, 0, size)

			for _, s := range src {
				offsets = append(offsets, uint32(len(values)))
				values = append(values, s...)
			}

			return enc.EncodeByteArray(dst, values, offsets)
		},

		func(enc encoding.Encoding, dst []string, src []byte) ([]string, error) {
			dst = dst[:0]

			values, offsets, err := enc.DecodeByteArray(nil, src, nil)
			if err != nil {
				return dst, err
			}

			if len(offsets) > 0 {
				baseOffset := offsets[0]

				for _, endOffset := range offsets[1:] {
					dst = append(dst, string(values[baseOffset:endOffset]))
					baseOffset = endOffset
				}
			}

			return dst, nil
		},
	)
}

type encodingFunc[T comparable] func(encoding.Encoding, []byte, []T) ([]byte, error)

type decodingFunc[T comparable] func(encoding.Encoding, []T, []byte) ([]T, error)

func encode[T comparable](f *testing.F, e encoding.Encoding, encode encodingFunc[T], decode decodingFunc[T]) {
	const bufferSize = 64 * 1024
	var zero T
	var err error
	var buf = make([]T, bufferSize/unsafe.Sizeof(zero))
	var dst = make([]byte, bufferSize)

	f.Fuzz(func(t *testing.T, src []T) {
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

		if !equal(buf, src) {
			t.Error("decoded output does not match the original input")
			return
		}
	})
}

func equal[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
