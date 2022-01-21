package bits_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
)

func TestPack(t *testing.T) {
	tests := [...]struct {
		scenario string
		src      []byte
		dst      []byte
		packed   int
		srcWidth uint
		dstWidth uint
	}{
		{
			scenario: "zero bits",
			src:      nil,
			dst:      nil,
			packed:   0,
			dstWidth: 4,
			srcWidth: 1,
		},

		{
			scenario: "1 bit words into 1 bit words",
			src: []byte{
				0b01010101, 0b10101010, 0b00001111, 0b11110000,
			},
			dst: []byte{
				0b01010101, 0b10101010, 0b00001111, 0b11110000,
			},
			packed:   32,
			dstWidth: 1,
			srcWidth: 1,
		},

		{
			scenario: "2 bits words into 2 bit words",
			src: []byte{
				0b01010101, 0b10101010, 0b00001111, 0b11110000,
			},
			dst: []byte{
				0b01010101, 0b10101010, 0b00001111, 0b11110000,
			},
			packed:   16,
			dstWidth: 2,
			srcWidth: 2,
		},

		{
			scenario: "4 bits words into 4 bit words",
			src: []byte{
				0b01010101, 0b10101010, 0b00001111, 0b11110000,
			},
			dst: []byte{
				0b01010101, 0b10101010, 0b00001111, 0b11110000,
			},
			packed:   8,
			dstWidth: 4,
			srcWidth: 4,
		},

		{
			scenario: "8 bits words into 8 bit words",
			src: []byte{
				0b01010101, 0b10101010, 0b00001111, 0b11110000,
				0b00000000, 0b11111111, 0b11110000, 0b00001111,
			},
			dst: []byte{
				0b01010101, 0b10101010, 0b00001111, 0b11110000,
				0b00000000, 0b11111111, 0b11110000, 0b00001111,
			},
			packed:   8,
			dstWidth: 8,
			srcWidth: 8,
		},

		{
			scenario: "3 bits words into 8 bits words",
			src: []byte{
				0b01010101, 0b00101010,
			},
			dst: []byte{
				0b00000101, 0b00000010, 0b00000001, 0b00000101,
				0b00000010,
			},
			packed:   5,
			dstWidth: 8,
			srcWidth: 3,
		},

		{
			scenario: "5 bits words into 16 bits words",
			src: []byte{
				0b00011111, 0b11110101,
			},
			dst: []byte{
				0b00011111, 0b00000000,
				0b00001000, 0b00000000,
				0b00011101, 0b00000000,
			},
			packed:   3,
			dstWidth: 16,
			srcWidth: 5,
		},

		{
			scenario: "2 bits words into 32 bits words",
			src: []byte{
				0b10101010, 0b11110101,
			},
			dst: []byte{
				0b00000010, 0b00000000, 0b00000000, 0b00000000,
				0b00000010, 0b00000000, 0b00000000, 0b00000000,
				0b00000010, 0b00000000, 0b00000000, 0b00000000,
				0b00000010, 0b00000000, 0b00000000, 0b00000000,

				0b00000001, 0b00000000, 0b00000000, 0b00000000,
				0b00000001, 0b00000000, 0b00000000, 0b00000000,
				0b00000011, 0b00000000, 0b00000000, 0b00000000,
				0b00000011, 0b00000000, 0b00000000, 0b00000000,
			},
			packed:   8,
			dstWidth: 32,
			srcWidth: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			buffer := make([]byte, len(test.dst))
			packed := bits.Pack(buffer, test.dstWidth, test.src, test.srcWidth)

			if packed != test.packed {
				t.Errorf("wrong number of words packed: want=%d got=%d", test.packed, packed)
			}

			if !bytes.Equal(buffer, test.dst) {
				t.Errorf("contents mismatch\nwant: %08b\ngot:  %08b", test.dst, buffer)
			} else {
				// Allocate the inverse buffer to the full buffer size to verify
				// that no extra bits get written at the end.
				inverse := make([]byte, len(buffer))
				inverted := bits.Pack(inverse, test.srcWidth, buffer, test.dstWidth)

				if inverted != test.packed {
					t.Errorf("wrong number of words inverted: want=%d got=%d", test.packed, inverted)
				}

				length := bits.ByteCount(uint(inverted) * test.srcWidth)
				inverse = inverse[:length]

				if (test.dstWidth % test.srcWidth) != 0 {
					// The source width is not a multiple of the destination
					// width, we copy the upper bits of the last byte of the
					// source to ensure the comparison does not fail due to
					// bytes that were not copied from the original input.
					mask := (1 << (test.srcWidth % 8)) - 1
					last := len(inverse) - 1
					inverse[last] |= test.src[last] & ^byte(mask)
				}

				if !bytes.Equal(inverse, test.src) {
					t.Errorf("contents mismatch\nwant: %08b\ngot:  %08b", test.src, inverse)
				}
			}
		})
	}
}

func BenchmarkPack(b *testing.B) {
	src := []byte{
		// 0:8
		0b00000001, 0b00000010, 0b00000100, 0b00001000,
		0b00010000, 0b00100000, 0b01000000, 0b10000000,

		// 8:16
		0b00000001, 0b00000010, 0b00000100, 0b00001000,
		0b00010000, 0b00100000, 0b01000000, 0b10000000,

		// 16:24
		0b00000001, 0b00000010, 0b00000100, 0b00001000,
		0b00010000, 0b00100000, 0b01000000, 0b10000000,

		// 24:32
		0b00000001, 0b00000010, 0b00000100, 0b00001000,
		0b00010000, 0b00100000, 0b01000000, 0b10000000,

		// 32:40
		0b00000001, 0b00000010, 0b00000100, 0b00001000,
		0b00010000, 0b00100000, 0b01000000, 0b10000000,

		// 40:48
		0b00000001, 0b00000010, 0b00000100, 0b00001000,
		0b00010000, 0b00100000, 0b01000000, 0b10000000,

		// 48:56
		0b00000001, 0b00000010, 0b00000100, 0b00001000,
		0b00010000, 0b00100000, 0b01000000, 0b10000000,

		// 56:64
		0b00000001, 0b00000010, 0b00000100, 0b00001000,
		0b00010000, 0b00100000, 0b01000000, 0b10000000,
	}

	const extraSpace = 10
	dst := make([]byte, 2*len(src)+extraSpace)

	for i := 0; i < b.N; i++ {
		n := bits.Pack(dst, 8, src, 4)

		if n != 2*len(src) {
			b.Errorf("wrong number of words packed: want=%d got=%d", 2*len(src), n)
		}
	}

	b.SetBytes(int64(len(dst)))
}
