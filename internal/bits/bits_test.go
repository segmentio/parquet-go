package bits_test

import (
	"fmt"
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestNearestPowerOfTwo(t *testing.T) {
	for _, test := range []struct {
		input  uint32
		output uint32
	}{
		{input: 0, output: 0},
		{input: 1, output: 1},
		{input: 2, output: 2},
		{input: 3, output: 4},
		{input: 4, output: 4},
		{input: 5, output: 8},
		{input: 6, output: 8},
		{input: 7, output: 8},
		{input: 8, output: 8},
		{input: 30, output: 32},
	} {
		t.Run(fmt.Sprintf("NearestPowerOfTwo(%d)", test.input), func(t *testing.T) {
			if nextPow2 := bits.NearestPowerOfTwo32(test.input); nextPow2 != test.output {
				t.Errorf("wrong 32 bits value: want=%d got=%d", test.output, nextPow2)
			}
			if nextPow2 := bits.NearestPowerOfTwo64(uint64(test.input)); nextPow2 != uint64(test.output) {
				t.Errorf("wrong 64 bits value: want=%d got=%d", test.output, nextPow2)
			}
		})
	}
}

func TestBitCount(t *testing.T) {
	for _, test := range []struct {
		bytes int
		bits  uint
	}{
		{bytes: 0, bits: 0},
		{bytes: 1, bits: 8},
		{bytes: 2, bits: 16},
		{bytes: 3, bits: 24},
		{bytes: 4, bits: 32},
		{bytes: 5, bits: 40},
		{bytes: 6, bits: 48},
	} {
		t.Run(fmt.Sprintf("BitCount(%d)", test.bytes), func(t *testing.T) {
			if bits := bits.BitCount(test.bytes); bits != test.bits {
				t.Errorf("wrong bit count: want=%d got=%d", test.bits, bits)
			}
		})
	}
}

func TestByteCount(t *testing.T) {
	for _, test := range []struct {
		bits  uint
		bytes int
	}{
		{bits: 0, bytes: 0},
		{bits: 1, bytes: 1},
		{bits: 7, bytes: 1},
		{bits: 8, bytes: 1},
		{bits: 9, bytes: 2},
		{bits: 30, bytes: 4},
		{bits: 63, bytes: 8},
	} {
		t.Run(fmt.Sprintf("ByteCount(%d)", test.bits), func(t *testing.T) {
			if bytes := bits.ByteCount(test.bits); bytes != test.bytes {
				t.Errorf("wrong byte count: want=%d got=%d", test.bytes, bytes)
			}
		})
	}
}

func TestRound(t *testing.T) {
	for _, test := range []struct {
		bits  uint
		round uint
	}{
		{bits: 0, round: 0},
		{bits: 1, round: 8},
		{bits: 8, round: 8},
		{bits: 9, round: 16},
		{bits: 30, round: 32},
		{bits: 63, round: 64},
	} {
		t.Run(fmt.Sprintf("Round(%d)", test.bits), func(t *testing.T) {
			if round := bits.Round(test.bits); round != test.round {
				t.Errorf("wrong rounded bit count: want=%d got=%d", test.round, round)
			}
		})
	}
}

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
