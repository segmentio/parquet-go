//go:build go1.18
// +build go1.18

package bytestreamsplit_test

import (
	"testing"

	"github.com/segmentio/parquet-go/encoding/bytestreamsplit"
	"github.com/segmentio/parquet-go/encoding/fuzz"
)

func FuzzEncodeFloat(f *testing.F) {
	fuzz.EncodeDouble(f, new(bytestreamsplit.Encoding))
}

func FuzzEncodeDouble(f *testing.F) {
	fuzz.EncodeDouble(f, new(bytestreamsplit.Encoding))
}
