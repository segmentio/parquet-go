//go:build purego || !amd64

package parquet

import (
	"encoding/binary"

	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func boundsInt32(data []int32) (min, max int32) {
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

func boundsInt64(data []int64) (min, max int64) {
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

func boundsUint32(data []uint32) (min, max uint32) {
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

func boundsUint64(data []uint64) (min, max uint64) {
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

func boundsFloat32(data []float32) (min, max float32) {
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

func boundsFloat64(data []float64) (min, max float64) {
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

func boundsBE128(data []byte) (min, max []byte) {
	if len(data) > 0 {
		be128 := unsafecast.BytesToUint128(data)
		minHi := binary.BigEndian.Uint64(be128[0][:8])
		maxHi := minHi
		minIndex := 0
		maxIndex := 0
		for i := 1; i < len(be128); i++ {
			hi := binary.BigEndian.Uint64(be128[i][:8])
			lo := binary.BigEndian.Uint64(be128[i][8:])
			switch {
			case hi < minHi:
				minHi, minIndex = hi, i
			case hi == minHi:
				minLo := binary.BigEndian.Uint64(be128[minIndex][8:])
				if lo < minLo {
					minHi, minIndex = hi, i
				}
			}
			switch {
			case hi > maxHi:
				maxHi, maxIndex = hi, i
			case hi == maxHi:
				maxLo := binary.BigEndian.Uint64(be128[maxIndex][8:])
				if lo > maxLo {
					maxHi, maxIndex = hi, i
				}
			}
		}
		min = be128[minIndex][:]
		max = be128[maxIndex][:]
	}
	return min, max
}
