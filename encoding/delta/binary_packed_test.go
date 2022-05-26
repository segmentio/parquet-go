//go:build amd64

package delta_test

import (
	"fmt"
	"testing"

	"github.com/segmentio/parquet-go/encoding/delta"
	"github.com/segmentio/parquet-go/encoding/test"
)

func TestEncodeInt32(t *testing.T) {
	const minNumValues = 0
	const maxNumValues = 100

	for bitWidth := uint(0); bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			test.EncodeInt32(t, new(delta.BinaryPackedEncoding), minNumValues, maxNumValues, bitWidth)
		})
	}
}
