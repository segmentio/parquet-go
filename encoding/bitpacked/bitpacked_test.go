//go:build go1.18
// +build go1.18

package bitpacked_test

import (
	"testing"

	"github.com/1712n/parquet-go/encoding/fuzz"
	"github.com/1712n/parquet-go/encoding/rle"
)

func FuzzEncodeLevels(f *testing.F) {
	fuzz.EncodeLevels(f, &rle.Encoding{BitWidth: 8})
}
