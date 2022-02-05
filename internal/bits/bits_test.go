package bits_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/segmentio/parquet-go/internal/bits"
)

const (
	bufferSize = 256 * 1024
)

func repeatBool(values []bool, n int) []bool {
	return bits.BytesToBool(
		bytes.Repeat(bits.BoolToBytes(values), n),
	)
}

func repeatInt32(values []int32, n int) []int32 {
	return bits.BytesToInt32(
		bytes.Repeat(bits.Int32ToBytes(values), n),
	)
}

func repeatInt64(values []int64, n int) []int64 {
	return bits.BytesToInt64(
		bytes.Repeat(bits.Int64ToBytes(values), n),
	)
}

func repeatUint32(values []uint32, n int) []uint32 {
	return bits.BytesToUint32(
		bytes.Repeat(bits.Uint32ToBytes(values), n),
	)
}

func repeatUint64(values []uint64, n int) []uint64 {
	return bits.BytesToUint64(
		bytes.Repeat(bits.Uint64ToBytes(values), n),
	)
}

func repeatFloat32(values []float32, n int) []float32 {
	return bits.BytesToFloat32(
		bytes.Repeat(bits.Float32ToBytes(values), n),
	)
}

func repeatFloat64(values []float64, n int) []float64 {
	return bits.BytesToFloat64(
		bytes.Repeat(bits.Float64ToBytes(values), n),
	)
}

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
