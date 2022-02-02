//go:build purego || !amd64

package bits

func minInt32(data []int32) (min int32) {
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

func minInt64(data []int64) (min int64) {
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

func minUint32(data []uint32) (min uint32) {
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

func minUint64(data []uint64) (min uint64) {
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

func minFloat32(data []float32) (min float32) {
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

func minFloat64(data []float64) (min float64) {
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
