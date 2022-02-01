package bits

import (
	"bytes"
)

func boolEqualAll(data []bool, value bool) bool {
	for i := range data {
		if data[i] != value {
			return false
		}
	}
	return len(data) > 0
}

func MinMaxBool(data []bool) (min, max bool) {
	if len(data) > 0 {
		switch {
		case boolEqualAll(data, true):
			min, max = true, true
		case boolEqualAll(data, false):
			min, max = false, false
		default:
			min, max = false, true
		}
	}
	return min, max
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
