package bits_test

import (
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestMinLeadingZeros32(t *testing.T) {
	for _, test := range []struct {
		data  []int32
		zeros int
	}{
		{
			data:  nil,
			zeros: 0,
		},

		{
			data:  []int32{0, 0, 0, 0, 0},
			zeros: 32,
		},

		{
			data: []int32{
				0x00000010,
				0x0000F000,
				0x00000990,
				0x00000000,
			},
			zeros: 16,
		},
	} {
		t.Run("", func(t *testing.T) {
			if zeros := bits.MinLeadingZeros32(test.data); zeros != test.zeros {
				t.Errorf("want=%d got=%d", test.zeros, zeros)
			}
		})
	}
}

func TestMinLeadingZeros64(t *testing.T) {
	for _, test := range []struct {
		data  []int64
		zeros int
	}{
		{
			data:  nil,
			zeros: 0,
		},

		{
			data:  []int64{0, 0, 0, 0, 0},
			zeros: 64,
		},

		{
			data: []int64{
				0x00000010,
				0x0000F000,
				0x00000990,
				0x00000000,
			},
			zeros: 48,
		},
	} {
		t.Run("", func(t *testing.T) {
			if zeros := bits.MinLeadingZeros64(test.data); zeros != test.zeros {
				t.Errorf("want=%d got=%d", test.zeros, zeros)
			}
		})
	}
}

func TestMinLeadingZeros96(t *testing.T) {
	for _, test := range []struct {
		data  [][12]byte
		zeros int
	}{
		{
			data:  nil,
			zeros: 0,
		},

		{
			data:  [][12]byte{{}, {}, {}, {}, {}},
			zeros: 96,
		},

		{
			data: [][12]byte{
				{0: 0x01},
				{10: 0xFF},
				{5: 0x02},
				{9: 0xF0},
			},
			zeros: 8,
		},
	} {
		t.Run("", func(t *testing.T) {
			if zeros := bits.MinLeadingZeros96(test.data); zeros != test.zeros {
				t.Errorf("want=%d got=%d", test.zeros, zeros)
			}
		})
	}
}
