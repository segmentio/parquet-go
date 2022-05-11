//go:build go1.18
// +build go1.18

package delta_test

import (
	"testing"

	"github.com/segmentio/parquet-go/encoding/delta"
	"github.com/segmentio/parquet-go/encoding/fuzz"
)

func FuzzDeltaBinaryPackedInt32(f *testing.F) {
	fuzz.EncodeInt32(f, new(delta.BinaryPackedEncoding))
}

func FuzzDeltaBinaryPackedInt64(f *testing.F) {
	fuzz.EncodeInt64(f, new(delta.BinaryPackedEncoding))
}

func FuzzDeltaLengthByteArray(f *testing.F) {
	fuzz.EncodeByteArray(f, new(delta.LengthByteArrayEncoding))
}

func FuzzDeltaByteArray(f *testing.F) {
	fuzz.EncodeByteArray(f, new(delta.ByteArrayEncoding))
}
