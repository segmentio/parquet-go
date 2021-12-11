package bits

import "bytes"

func MinBool(data []bool) (min bool) {
	return boolEqualAll(data, true)
}

func MinInt32(data []int32) (min int32) {
	if len(data) > 0 {
		min = data[0]

		for _, value := range data {
			if value < min {
				min = value
			}
		}
	}
	return min
}

func MinInt64(data []int64) (min int64) {
	if len(data) > 0 {
		min = data[0]

		for _, value := range data {
			if value < min {
				min = value
			}
		}
	}
	return min
}

func MinUint32(data []uint32) (min uint32) {
	if len(data) > 0 {
		min = data[0]

		for _, value := range data {
			if value < min {
				min = value
			}
		}
	}
	return min
}

func MinUint64(data []uint64) (min uint64) {
	if len(data) > 0 {
		min = data[0]

		for _, value := range data {
			if value < min {
				min = value
			}
		}
	}
	return min
}

func MinFloat32(data []float32) (min float32) {
	if len(data) > 0 {
		min = data[0]

		for _, value := range data {
			if value < min {
				min = value
			}
		}
	}
	return min
}

func MinFloat64(data []float64) (min float64) {
	if len(data) > 0 {
		min = data[0]

		for _, value := range data {
			if value < min {
				min = value
			}
		}
	}
	return min
}

func MinFixedLenByteArray(size int, data []byte) (min []byte) {
	if len(data) > 0 {
		min = data[:size]

		for i, j := size, 2*size; j <= len(data); {
			item := data[i:j]

			if bytes.Compare(item, min) < 0 {
				min = item
			}

			i += size
			j += size
		}
	}
	return min
}
