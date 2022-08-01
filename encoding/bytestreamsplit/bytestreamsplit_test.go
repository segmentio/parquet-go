//go:build go1.18
// +build go1.18

package bytestreamsplit_test

import (
	"testing"

	"github.com/segmentio/parquet-go/encoding/bytestreamsplit"
	"github.com/segmentio/parquet-go/encoding/fuzz"
	"github.com/segmentio/parquet-go/encoding/test"
)

func FuzzEncodeFloat(f *testing.F) {
	fuzz.EncodeFloat(f, new(bytestreamsplit.Encoding))
}

func FuzzEncodeDouble(f *testing.F) {
	fuzz.EncodeDouble(f, new(bytestreamsplit.Encoding))
}

func TestEncodeFloat(t *testing.T) {
	test.EncodeFloat(t, new(bytestreamsplit.Encoding), 0, 100)
}

func TestEncodeDouble(t *testing.T) {
	test.EncodeDouble(t, new(bytestreamsplit.Encoding), 0, 100)
}
