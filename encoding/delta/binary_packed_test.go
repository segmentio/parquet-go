//go:build amd64

package delta_test

import (
	"fmt"
	"testing"

	"github.com/segmentio/parquet-go/encoding/delta"
	"github.com/segmentio/parquet-go/encoding/test"
)

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
