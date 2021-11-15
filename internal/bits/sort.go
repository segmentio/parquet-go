package bits

import (
	"bytes"
	"sort"

	"github.com/segmentio/asm/qsort"
)

func SortInt32(data []int32) {
	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
}

func SortInt64(data []int64) {
	qsort.Sort(Int64ToBytes(data), 8, nil)
}

func SortInt96(data [][12]byte) {
	sort.Slice(data, func(i, j int) bool { return CompareInt96(data[i], data[j]) < 0 })
}

func SortFloat32(data []float32) {
	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
}

func SortFloat64(data []float64) {
	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
}

func SortByteArray(data [][]byte) {
	sort.Slice(data, func(i, j int) bool { return bytes.Compare(data[i], data[j]) < 0 })
}

func SortFixedLenByteArray(size int, data []byte) {
	qsort.Sort(data, size, nil)
}
