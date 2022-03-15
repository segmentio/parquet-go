package bits

import (
	"bytes"
	"unsafe"
)

func OrderOfBool(data []bool) int {
	switch len(data) {
	case 0:
		return 0
	case 1:
		return 1
	default:
		k := 0
		i := 0

		if data[0] { // true => false: descending
			k = -1
			i = streakOfTrue(data)
			if i == len(data) {
				k = +1
			} else {
				i += streakOfFalse(data[i:])
			}
		} else { // false => true: ascending
			k = +1
			i = streakOfFalse(data)
			i += streakOfTrue(data[i:])
		}

		if i != len(data) {
			k = 0
		}
		return k
	}
}

func streakOfTrue(data []bool) int {
	if i := bytes.IndexByte(boolToBytes(data), 0); i >= 0 {
		return i
	}
	return len(data)
}

func streakOfFalse(data []bool) int {
	if i := bytes.IndexByte(boolToBytes(data), 1); i >= 0 {
		return i
	}
	return len(data)
}

func boolToBytes(data []bool) []byte {
	return *(*[]byte)(unsafe.Pointer(&data))
}

func OrderOfInt32(data []int32) int {
	switch len(data) {
	case 0:
		return 0
	case 1:
		return 1
	default:
		return orderOfInt32(data)
	}
}

func OrderOfInt64(data []int64) int {
	switch len(data) {
	case 0:
		return 0
	case 1:
		return 1
	default:
		return orderOfInt64(data)
	}
}

func OrderOfUint32(data []uint32) int {
	switch len(data) {
	case 0:
		return 0
	case 1:
		return 1
	default:
		return orderOfUint32(data)
	}
}

func OrderOfUint64(data []uint64) int {
	switch len(data) {
	case 0:
		return 0
	case 1:
		return 1
	default:
		return orderOfUint64(data)
	}
}

func OrderOfFloat32(data []float32) int {
	switch len(data) {
	case 0:
		return 0
	case 1:
		return 1
	default:
		return orderOfFloat32(data)
	}
}

func OrderOfFloat64(data []float64) int {
	switch len(data) {
	case 0:
		return 0
	case 1:
		return 1
	default:
		return orderOfFloat64(data)
	}
}

func OrderOfBytes(data [][]byte) int {
	if len(data) == 0 {
		return 0
	}
	if len(data) == 1 {
		return 1
	}
	data = skipBytesStreak(data)
	if len(data) < 2 {
		return 1
	}
	ordering := bytes.Compare(data[0], data[1])
	switch {
	case ordering > 0:
		if bytesAreInAscendingOrder(data[1:]) {
			return +1
		}
	case ordering < 0:
		if bytesAreInDescendingOrder(data[1:]) {
			return -1
		}
	}
	return 0
}

func skipBytesStreak(data [][]byte) [][]byte {
	for i := 1; i < len(data); i++ {
		if !bytes.Equal(data[i], data[0]) {
			return data[i-1:]
		}
	}
	return data[len(data)-1:]
}

func bytesAreInAscendingOrder(data [][]byte) bool {
	for i := len(data) - 1; i > 0; i-- {
		k := bytes.Compare(data[i-1], data[i])
		if k > 0 {
			return false
		}
	}
	return true
}

func bytesAreInDescendingOrder(data [][]byte) bool {
	for i := len(data) - 1; i > 0; i-- {
		k := bytes.Compare(data[i-1], data[i])
		if k < 0 {
			return false
		}
	}
	return true
}
