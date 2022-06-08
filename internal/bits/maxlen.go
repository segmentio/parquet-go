package bits

import "math/bits"

func MaxLen8(data []int8) int {
	max := 0
	for _, v := range data {
		if n := bits.Len8(uint8(v)); n > max {
			max = n
		}
	}
	return max
}

func MaxLen16(data []int16) int {
	max := 0
	for _, v := range data {
		if n := bits.Len16(uint16(v)); n > max {
			max = n
		}
	}
	return max
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
