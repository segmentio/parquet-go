package bits

import (
	"math/bits"
	"unsafe"
)

func MaxLen8(data []int8) int {
	max := 1
	for _, v := range data {
		if n := Len8(v); n > max {
			max = n
		}
	}
	return max
}

func MaxLen16(data []int16) int {
	max := 1
	for _, v := range data {
		if n := Len16(v); n > max {
			max = n
		}
	}
	return max
}

func MaxLen32(data []int32) int {
	max := 1
	for _, v := range data {
		if n := Len32(v); n > max {
			max = n
		}
	}
	return max
}

func MaxLen64(data []int64) int {
	max := 1
	for _, v := range data {
		if n := Len64(v); n > max {
			max = n
		}
	}
	return max
}

func MaxLen96(data [][12]byte) int {
	max := 1
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
