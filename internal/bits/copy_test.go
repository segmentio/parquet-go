package bits_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestCopy(t *testing.T) {
	tests := [...]struct {
		scenario string
		src      []byte
		dst      []byte
		count    uint
		shift    uint
	}{
		{
			scenario: "zero bits",
			src:      nil,
			dst:      nil,
			count:    0,
			shift:    0,
		},

		{
			scenario: "one bit at a zero offset",
			src:      []byte{0b11111111},
			dst:      []byte{0b00000001},
			count:    1,
			shift:    0,
		},

		{
			scenario: "one bit at a non-zero offset",
			src:      []byte{0b00001000},
			dst:      []byte{0b00000001},
			count:    1,
			shift:    3,
		},

		{
			scenario: "two bits at a zero offset",
			src:      []byte{0b11111111},
			dst:      []byte{0b00000011},
			count:    2,
			shift:    0,
		},

		{
			scenario: "two bits at a non-zero offset",
			src:      []byte{0b01100000},
			dst:      []byte{0b00000011},
			count:    2,
			shift:    5,
		},

		{
			scenario: "twelve bits at a zero offset",
			src:      []byte{0b10000001, 0b11111111},
			dst:      []byte{0b10000001, 0b00001111},
			count:    12,
			shift:    0,
		},

		{
			scenario: "twelve bits at a non-zero offset",
			src:      []byte{0b10000001, 0b00010101},
			dst:      []byte{0b11000000, 0b00001010},
			count:    12,
			shift:    1,
		},

		{
			scenario: "sixty-four bits at a zero offset",
			src: []byte{
				0b00000001,
				0b00000010,
				0b00000100,
				0b00001000,
				0b00010000,
				0b00100000,
				0b01000000,
				0b10000000,
				0b00000000,
			},
			dst: []byte{
				0b00000001,
				0b00000010,
				0b00000100,
				0b00001000,
				0b00010000,
				0b00100000,
				0b01000000,
				0b10000000,
				0b00000000,
			},
			count: 64,
			shift: 0,
		},

		{
			scenario: "sixty-four bits at a non-zero offset",
			src: []byte{
				0b00000001,
				0b00000010,
				0b00000100,
				0b00001000,
				0b00010000,
				0b00100000,
				0b01000000,
				0b10000000,
				0b00000000,
			},
			dst: []byte{
				0b01000000,
				0b10000000,
				0b00000000,
				0b00000001,
				0b00000010,
				0b00000100,
				0b00001000,
				0b00010000,
				0b00000000,
			},
			count: 64,
			shift: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			buffer := make([]byte, len(test.dst))
			copied := bits.Copy(buffer, test.src, test.shift, test.count)

			if copied != int(test.count) {
				t.Errorf("wrong number of bits copied: want=%d got=%d", test.count, copied)
			}

			if !bytes.Equal(buffer, test.dst) {
				t.Error("contents mismatch")
				t.Logf("want:\n%08b", test.dst)
				t.Logf("got:\n%08b", buffer)
			}
		})
	}
}

func BenchmarkCopy(b *testing.B) {
	src := []byte{
		0b00000001,
		0b00000010,
		0b00000100,
		0b00001000,
		0b00010000,
		0b00100000,
		0b01000000,
		0b10000000,
		0b00000000,
	}

	dst := make([]byte, 8)

	for i := 0; i < b.N; i++ {
		bits.Copy(dst, src, 3, 64)
	}

	b.SetBytes(int64(len(dst)))
}
