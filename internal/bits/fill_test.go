package bits_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
)

func TestFill(t *testing.T) {
	tests := [...]struct {
		src      uint64
		srcWidth uint
		dstWidth uint
		output   []byte
	}{
		{
			src:      0b00000000,
			srcWidth: 1,
			dstWidth: 1,
			output:   []byte{},
		},

		{
			src:      0b00000000,
			srcWidth: 1,
			dstWidth: 1,
			output:   []byte{0},
		},

		{
			src:      0b00000001,
			srcWidth: 1,
			dstWidth: 1,
			output:   []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},

		{
			src:      0b00000001,
			srcWidth: 1,
			dstWidth: 2,
			output: []byte{
				0b01010101,
				0b01010101,
				0b01010101,
				0b01010101,
			},
		},

		{
			src:      0b00001111,
			srcWidth: 5,
			dstWidth: 8,
			output: []byte{
				0b00001111,
				0b00001111,
			},
		},

		{
			src:      0b00000101,
			srcWidth: 3,
			dstWidth: 3,
			output: []byte{
				0b01101101, 0b11011011, 0b10110110,
				0b01101101, 0b11011011, 0b10110110,
			},
		},

		{
			src:      0b00000101,
			srcWidth: 3,
			dstWidth: 8,
			output: []byte{
				0b00000101, 0b00000101, 0b00000101,
				0b00000101, 0b00000101, 0b00000101,
				0b00000101, 0b00000101, 0b00000101,
				0b00000101, 0b00000101, 0b00000101,
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			b := make([]byte, len(test.output))
			n := bits.Fill(b, test.dstWidth, test.src, test.srcWidth)

			if n != int(bits.BitCount(len(b))/test.dstWidth) {
				t.Error("invalid length:", n)
			}

			if !bytes.Equal(b, test.output) {
				t.Errorf("content mismatch:\n%08b\n%08b", test.output, b)
			}
		})
	}
}

func BenchmarkFill(b *testing.B) {
	buffer := make([]byte, 4096)

	for i := 0; i < b.N; i++ {
		bits.Fill(buffer, 8, 0x2a, 6)
	}

	b.SetBytes(int64(len(buffer)))
}
