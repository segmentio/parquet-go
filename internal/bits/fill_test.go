package bits_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestFill(t *testing.T) {
	tests := [...]struct {
		input  []byte
		output []byte
	}{
		{
			input:  nil,
			output: nil,
		},

		{
			input:  []byte{1},
			output: []byte{1},
		},

		{
			input:  []byte{1},
			output: []byte{1, 1, 1, 1, 1, 1, 1, 1, 1},
		},

		{
			input:  []byte{1, 2, 3},
			output: []byte{1, 2, 3, 1, 2, 3, 1, 2, 3},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			b := make([]byte, len(test.output))
			n := bits.Fill(b, test.input)

			if n != len(b) {
				t.Error("invalid length:", n)
			}

			if !bytes.Equal(b, test.output) {
				t.Error("content mismatch")
			}
		})
	}
}

func BenchmarkFill(b *testing.B) {
	buffer := make([]byte, 4096)
	pattern := []byte{0, 1, 2, 3}

	for i := 0; i < b.N; i++ {
		bits.Fill(buffer, pattern)
	}

	b.SetBytes(int64(len(buffer)))
}
