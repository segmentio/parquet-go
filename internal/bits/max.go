package bits

import "bytes"

func MaxBool(data []bool) (max bool) {
	return len(data) > 0 && !boolEqualAll(data, false)
}

func MaxInt32(data []int32) (max int32) {
	if len(data) > 0 {
		max = data[0]

		for _, value := range data {
			if value > max {
				max = value
			}
		}
	}
	return max
}

func MaxInt64(data []int64) (max int64) {
	if len(data) > 0 {
		max = data[0]

		for _, value := range data {
			if value > max {
				max = value
			}
		}
	}
	return max
}

func MaxUint32(data []uint32) (max uint32) {
	if len(data) > 0 {
		max = data[0]

		for _, value := range data {
			if value > max {
				max = value
			}
		}
	}
	return max
}

func MaxUint64(data []uint64) (max uint64) {
	if len(data) > 0 {
		max = data[0]

		for _, value := range data {
			if value > max {
				max = value
			}
		}
	}
	return max
}

func MaxFloat32(data []float32) (max float32) {
	if len(data) > 0 {
		max = data[0]

		for _, value := range data {
			if value > max {
				max = value
			}
		}
	}
	return max
}

func MaxFloat64(data []float64) (max float64) {
	if len(data) > 0 {
		max = data[0]

		for _, value := range data {
			if value > max {
				max = value
			}
		}
	}
	return max
}

func MaxFixedLenByteArray(size int, data []byte) (max []byte) {
	if len(data) > 0 {
		max = data[:size]

		for i, j := size, 2*size; j <= len(data); {
			item := data[i:j]

			if bytes.Compare(item, max) > 0 {
				max = item
			}

			i += size
			j += size
		}
	}
	return max
}
