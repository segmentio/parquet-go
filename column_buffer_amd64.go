//go:build !purego

package parquet

import "unsafe"

//go:noescape
func writeValuesBitpack(values []byte, rows array, size, offset uintptr)

//go:noescape
func writeValuesInt32(values []int32, rows array, size, offset uintptr)

func writeValuesInt64(values []int64, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*int64)(rows.index(i, size, offset))
	}
}

func writeValuesUint32(values []uint32, rows array, size, offset uintptr) {
	writeValuesInt32(*(*[]int32)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesUint64(values []uint64, rows array, size, offset uintptr) {
	writeValuesInt64(*(*[]int64)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesFloat32(values []float32, rows array, size, offset uintptr) {
	writeValuesInt32(*(*[]int32)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesFloat64(values []float64, rows array, size, offset uintptr) {
	writeValuesInt64(*(*[]int64)(unsafe.Pointer(&values)), rows, size, offset)
}
