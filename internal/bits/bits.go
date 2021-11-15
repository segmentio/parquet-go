package bits

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"unsafe"
)

func BitCount(count int) uint {
	return 8 * uint(count)
}

func ByteCount(count uint) int {
	return int((count + 7) / 8)
}

func IndexShift8(bitIndex uint) (index, shift uint) {
	return bitIndex / 8, bitIndex % 8
}

func IndexShift16(bitIndex uint) (index, shift uint) {
	return bitIndex / 16, bitIndex % 16
}

func IndexShift32(bitIndex uint) (index, shift uint) {
	return bitIndex / 32, bitIndex % 32
}

func IndexShift64(bitIndex uint) (index, shift uint) {
	return bitIndex / 64, bitIndex % 64
}

func MaxLen32(data []int32) int {
	max := 0
	for _, v := range data {
		if n := bits.Len32(uint32(v)); n > max {
			max = n
		}
	}
	return max
}

func MaxLen64(data []int64) int {
	max := 0
	for _, v := range data {
		if n := bits.Len64(uint64(v)); n > max {
			max = n
		}
	}
	return max
}

func MaxLen96(data [][12]byte) int {
	max := 0
	for i := range data {
		p := unsafe.Pointer(&data[i][0])
		// assume little endian
		hi := *(*uint64)(unsafe.Add(p, 4))
		lo := *(*uint32)(p)
		switch {
		case hi != 0:
			if n := bits.Len64(hi) + 32; n > max {
				max = n
			}
		case lo != 0:
			if n := bits.Len32(lo); n > max {
				max = n
			}
		}
	}
	return max
}

func MinLeadingZeros32(data []int32) int {
	if len(data) == 0 {
		return 0
	}
	return 32 - MaxLen32(data)
}

func MinLeadingZeros64(data []int64) int {
	if len(data) == 0 {
		return 0
	}
	return 64 - MaxLen64(data)
}

func MinLeadingZeros96(data [][12]byte) int {
	if len(data) == 0 {
		return 0
	}
	return 96 - MaxLen96(data)
}

func MinMaxInt32(data []int32) (min, max int32) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]

		for _, v := range data[1:] {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	return min, max
}

func MinMaxInt64(data []int64) (min, max int64) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]

		for _, v := range data[1:] {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	return min, max
}

func MinMaxInt96(data [][12]byte) (min, max [12]byte) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]

		for _, v := range data[1:] {
			if CompareInt96(v, min) < 0 {
				min = v
			}
			if CompareInt96(v, max) > 0 {
				max = v
			}
		}
	}
	return min, max
}

func MinMaxUint32(data []uint32) (min, max uint32) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]

		for _, v := range data[1:] {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	return min, max
}

func MinMaxUint64(data []uint64) (min, max uint64) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]

		for _, v := range data[1:] {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	return min, max
}

func MinMaxFloat32(data []float32) (min, max float32) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]

		for _, v := range data[1:] {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	return min, max
}

func MinMaxFloat64(data []float64) (min, max float64) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]

		for _, v := range data[1:] {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	return min, max
}

func MinMaxByteArray(data [][]byte) (min, max []byte) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]

		for _, v := range data {
			if bytes.Compare(v, min) < 0 {
				min = v
			}
			if bytes.Compare(v, max) > 0 {
				max = v
			}
		}
	}
	return min, max
}

func MinMaxFixedLenByteArray(size int, data []byte) (min, max []byte) {
	if len(data) > 0 {
		min = data[:size]
		max = data[:size]

		for i, j := size, 2*size; j <= len(data); {
			item := data[i:j]

			if bytes.Compare(item, min) < 0 {
				min = item
			}
			if bytes.Compare(item, max) > 0 {
				max = item
			}

			i += size
			j += size
		}
	}
	return min, max
}

func CompareInt96(v1, v2 [12]byte) int {
	hi1 := binary.LittleEndian.Uint32(v1[8:])
	hi2 := binary.LittleEndian.Uint32(v2[8:])

	switch {
	case hi1 < hi2:
		return -1
	case hi1 > hi2:
		return +1
	}

	lo1 := binary.LittleEndian.Uint64(v1[:8])
	lo2 := binary.LittleEndian.Uint64(v2[:8])

	switch {
	case lo1 < lo2:
		return -1
	case lo1 > lo2:
		return +1
	default:
		return 0
	}
}
