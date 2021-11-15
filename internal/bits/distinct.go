package bits

import "bytes"

func CountDistinctInt32(sortedData []int32) int {
	distinctCount := 0

	for i := 0; i < len(sortedData); {
		j := i + 1

		for j < len(sortedData) && sortedData[i] == sortedData[j] {
			j++
		}

		i = j
		distinctCount++
	}

	return distinctCount
}

func CountDistinctInt64(sortedData []int64) int {
	distinctCount := 0

	for i := 0; i < len(sortedData); {
		j := i + 1

		for j < len(sortedData) && sortedData[i] == sortedData[j] {
			j++
		}

		i = j
		distinctCount++
	}

	return distinctCount
}

func CountDistinctInt96(sortedData [][12]byte) int {
	distinctCount := 0

	for i := 0; i < len(sortedData); {
		j := i + 1

		for j < len(sortedData) && sortedData[i] == sortedData[j] {
			j++
		}

		i = j
		distinctCount++
	}

	return distinctCount
}

func CountDistinctFloat32(sortedData []float32) int {
	distinctCount := 0

	for i := 0; i < len(sortedData); {
		j := i + 1

		for j < len(sortedData) && sortedData[i] == sortedData[j] {
			j++
		}

		i = j
		distinctCount++
	}

	return distinctCount
}

func CountDistinctFloat64(sortedData []float64) int {
	distinctCount := 0

	for i := 0; i < len(sortedData); {
		j := i + 1

		for j < len(sortedData) && sortedData[i] == sortedData[j] {
			j++
		}

		i = j
		distinctCount++
	}

	return distinctCount
}

func CountDistinctByteArray(sortedData [][]byte) int {
	distinctCount := 0

	for i := 0; i < len(sortedData); {
		j := i + 1

		for j < len(sortedData) && bytes.Equal(sortedData[i], sortedData[j]) {
			j++
		}

		i = j
		distinctCount++
	}

	return distinctCount
}

func CountDistinctFixedLenByteArray(size int, sortedData []byte) int {
	distinctCount := 0

	for i := 0; i < len(sortedData); {
		j := i + size
		b := sortedData[i:j]

		for j < len(sortedData) && bytes.Equal(b, sortedData[j:j+size]) {
			j += size
		}

		i = j
		distinctCount++
	}

	return distinctCount
}
