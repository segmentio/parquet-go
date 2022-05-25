//go:build !purego

package parquet

import "unsafe"

//go:noescape
func writeValuesBitpack(values []byte, rows array, size, offset uintptr)

//go:noescape
func writeValues32bits(values []int32, rows array, size, offset uintptr)

func writeValues64bits(values []int64, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*int64)(rows.index(i, size, offset))
	}
}

func writeValuesInt32(values []int32, rows array, size, offset uintptr) {
	writeValues32bits(values, rows, size, offset)
}

func writeValuesInt64(values []int64, rows array, size, offset uintptr) {
	writeValues64bits(values, rows, size, offset)
}

func writeValuesUint32(values []uint32, rows array, size, offset uintptr) {
	writeValues32bits(*(*[]int32)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesUint64(values []uint64, rows array, size, offset uintptr) {
	writeValues64bits(*(*[]int64)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesFloat32(values []float32, rows array, size, offset uintptr) {
	writeValues32bits(*(*[]int32)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesFloat64(values []float64, rows array, size, offset uintptr) {
	writeValues64bits(*(*[]int64)(unsafe.Pointer(&values)), rows, size, offset)
}
