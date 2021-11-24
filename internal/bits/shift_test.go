package bits_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestShiftRight(t *testing.T) {
	tests := []struct {
		shift  uint
		input  []byte
		output []byte
	}{
		{
			shift:  0,
			input:  []byte{0b00001111, 0b00001111, 0b00001111, 0b00001111},
			output: []byte{0b00001111, 0b00001111, 0b00001111, 0b00001111},
		},

		{
			shift:  1,
			input:  []byte{0b00001111, 0b00001111, 0b00001111, 0b00001111},
			output: []byte{0b10000111, 0b10000111, 0b10000111, 0b00000111},
		},

		{
			shift:  2,
			input:  []byte{0b00001111, 0b00001111, 0b00001111, 0b00001111},
			output: []byte{0b11000011, 0b11000011, 0b11000011, 0b00000011},
		},

		{
			shift:  3,
			input:  []byte{0b00001111, 0b00001111, 0b00001111, 0b00001111},
			output: []byte{0b11100001, 0b11100001, 0b11100001, 0b00000001},
		},

		{
			shift:  4,
			input:  []byte{0b00001111, 0b00001111, 0b00001111, 0b00001111},
			output: []byte{0b11110000, 0b11110000, 0b11110000, 0b00000000},
		},
	}

	buffer := []byte{}

	for _, test := range tests {
		t.Run(fmt.Sprintf("shift=%d", test.shift), func(t *testing.T) {
			buffer = append(buffer[:0], test.input...)
			bits.ShiftRight(buffer, test.shift)

			if !bytes.Equal(buffer, test.output) {
				t.Errorf("output mismatch:\n%08b\n%08b", test.output, buffer)
			}
		})
	}
}

func BenchmarkShiftRight(b *testing.B) {
	buffer := make([]byte, 4096)

	for i := 0; i < b.N; i++ {
		bits.ShiftRight(buffer, uint(i)%8)
	}

	b.SetBytes(int64(len(buffer)))
}
