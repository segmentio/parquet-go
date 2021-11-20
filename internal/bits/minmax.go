package bits

import (
	"bytes"
)

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

		for _, v := range data[1:] {
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
