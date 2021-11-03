package bits_test

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestUnpack(t *testing.T) {
	tests := [...]struct {
		scenario string
		src      []byte
		dst      []byte
		unpacked int
		srcWidth uint
		dstWidth uint
	}{
		{
			scenario: "zero bits",
			src:      nil,
			dst:      nil,
			unpacked: 0,
			dstWidth: 4,
			srcWidth: 1,
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
			unpacked: 5,
			dstWidth: 8,
			srcWidth: 3,
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
			unpacked: 8,
			dstWidth: 32,
			srcWidth: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			buffer := make([]byte, len(test.dst))
			unpacked := bits.Unpack(buffer, test.dstWidth, test.src, test.srcWidth)

			if unpacked != test.unpacked {
				t.Errorf("wrong number of words unpacked: want=%d got=%d", test.unpacked, unpacked)
			}

			if !bytes.Equal(buffer, test.dst) {
				t.Error("contents mismatch")
				t.Logf("want:\n%08b", test.dst)
				t.Logf("got:\n%08b", buffer)
			}
		})
	}
}

func BenchmarkUnpack(b *testing.B) {
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

	dst := make([]byte, 2*len(src))

	for i := 0; i < b.N; i++ {
		n := bits.Unpack(dst, 8, src, 4)

		if n != len(dst) {
			b.Errorf("wrong number of words unpacked: want=%d got=%d", len(dst), n)
		}
	}

	b.SetBytes(int64(len(dst)))
}
