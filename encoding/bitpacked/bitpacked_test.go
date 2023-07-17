package bitpacked_test

import (
	"testing"

	"github.com/parquet-go/parquet-go/encoding/fuzz"
	"github.com/parquet-go/parquet-go/encoding/rle"
)

func FuzzEncodeLevels(f *testing.F) {
	fuzz.EncodeLevels(f, &rle.Encoding{BitWidth: 8})
}
