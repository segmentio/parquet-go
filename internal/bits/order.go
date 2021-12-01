package bits

import "bytes"

func OrderOfBool(data []bool) int {
	k := 0

	if len(data) > 0 {
		i := 0

		if data[0] { // true => false: descending
			k = +1
			i = strakeOfTrue(data)
			i += strakeOfFalse(data[i:])
		} else { // false => true: ascending
			k = -1
			i = strakeOfFalse(data)
			i += strakeOfTrue(data[i:])
		}

		if i != len(data) {
			k = 0
		}
	}

	return k
}

func OrderOfInt32(data []int32) int {
	if len(data) > 0 {
		if int32AreInAscendingOrder(data) {
			return +1
		}
		if int32AreInDescendingOrder(data) {
			return -1
		}
	}
	return 0
}

func OrderOfInt64(data []int64) int {
	if len(data) > 0 {
		if int64AreInAscendingOrder(data) {
			return +1
		}
		if int64AreInDescendingOrder(data) {
			return -1
		}
	}
	return 0
}

func OrderOfUint32(data []uint32) int {
	if len(data) > 0 {
		if uint32AreInAscendingOrder(data) {
			return +1
		}
		if uint32AreInDescendingOrder(data) {
			return -1
		}
	}
	return 0
}

func OrderOfUint64(data []uint64) int {
	if len(data) > 0 {
		if uint64AreInAscendingOrder(data) {
			return +1
		}
		if uint64AreInDescendingOrder(data) {
			return -1
		}
	}
	return 0
}

func OrderOfFloat32(data []float32) int {
	if len(data) > 0 {
		if float32AreInAscendingOrder(data) {
			return +1
		}
		if float32AreInDescendingOrder(data) {
			return -1
		}
	}
	return 0
}

func OrderOfFloat64(data []float64) int {
	if len(data) > 0 {
		if float64AreInAscendingOrder(data) {
			return +1
		}
		if float64AreInDescendingOrder(data) {
			return -1
		}
	}
	return 0
}

func OrderOfBytes(data [][]byte) int {
	if len(data) == 0 {
		return 0
	}
	if len(data) == 1 {
		return 1
	}
	k := bytes.Compare(data[len(data)-2], data[len(data)-1])
	for i := len(data) - 2; i > 0; i-- {
		if bytes.Compare(data[i-1], data[i]) != k {
			return 0
		}
	}
	return k
}

func strakeOfTrue(data []bool) int {
	if i := bytes.IndexByte(BoolToBytes(data), 0); i >= 0 {
		return i
	}
	return len(data)
}

func strakeOfFalse(data []bool) int {
	if i := bytes.IndexByte(BoolToBytes(data), 1); i >= 0 {
		return i
	}
	return len(data)
}

// generics please! :'(

func int32AreInAscendingOrder(data []int32) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] > data[i] {
			return false
		}
	}
	return true
}

func int32AreInDescendingOrder(data []int32) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] < data[i] {
			return false
		}
	}
	return true
}

func int64AreInAscendingOrder(data []int64) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] > data[i] {
			return false
		}
	}
	return true
}

func int64AreInDescendingOrder(data []int64) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] < data[i] {
			return false
		}
	}
	return true
}

func uint32AreInAscendingOrder(data []uint32) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] > data[i] {
			return false
		}
	}
	return true
}

func uint32AreInDescendingOrder(data []uint32) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] < data[i] {
			return false
		}
	}
	return true
}

func uint64AreInAscendingOrder(data []uint64) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] > data[i] {
			return false
		}
	}
	return true
}

func uint64AreInDescendingOrder(data []uint64) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] < data[i] {
			return false
		}
	}
	return true
}

func float32AreInAscendingOrder(data []float32) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] > data[i] {
			return false
		}
	}
	return true
}

func float32AreInDescendingOrder(data []float32) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] < data[i] {
			return false
		}
	}
	return true
}

func float64AreInAscendingOrder(data []float64) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] > data[i] {
			return false
		}
	}
	return true
}

func float64AreInDescendingOrder(data []float64) bool {
	for i := len(data) - 1; i > 0; i-- {
		if data[i-1] < data[i] {
			return false
		}
	}
	return true
}
