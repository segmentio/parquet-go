//go:build go1.18
// +build go1.18

package delta_test

import (
	"fmt"
	"testing"

	"github.com/segmentio/parquet-go/encoding/delta"
	"github.com/segmentio/parquet-go/encoding/fuzz"
	"github.com/segmentio/parquet-go/encoding/test"
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

const (
	encodeMinNumValues = 0
	encodeMaxNumValues = 200
)

func TestEncodeInt32(t *testing.T) {
	for bitWidth := uint(0); bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			test.EncodeInt32(t,
				new(delta.BinaryPackedEncoding),
				encodeMinNumValues,
				encodeMaxNumValues,
				bitWidth,
			)
		})
	}
}

func TestEncodeInt64(t *testing.T) {
	for bitWidth := uint(0); bitWidth <= 64; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			test.EncodeInt64(t,
				new(delta.BinaryPackedEncoding),
				encodeMinNumValues,
				encodeMaxNumValues,
				bitWidth,
			)
		})
	}
}
