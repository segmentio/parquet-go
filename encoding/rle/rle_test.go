//go:build go1.18
// +build go1.18

package rle_test

import (
	"testing"

	"github.com/segmentio/parquet-go/encoding/fuzz"
	"github.com/segmentio/parquet-go/encoding/rle"
)

func FuzzEncodeBoolean(f *testing.F) {
	fuzz.EncodeBoolean(f, &rle.Encoding{BitWidth: 1})
}

func FuzzEncodeInt8(f *testing.F) {
	fuzz.EncodeInt8(f, &rle.Encoding{BitWidth: 8})
}

func FuzzEncodeInt32(f *testing.F) {
	fuzz.EncodeInt32(f, &rle.Encoding{BitWidth: 32})
}
