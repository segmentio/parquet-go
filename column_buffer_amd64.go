//go:build !purego

package parquet

import (
	"unsafe"
)

func broadcastValueInt32(dst []int32, src int8) {
	memset(unsafe.Slice(*(**byte)(unsafe.Pointer(&dst)), len(dst)*4), byte(src))
}

//go:noescape
func broadcastRangeInt32(dst []int32, base int32)

//go:noescape
func writeValuesBitpack(values unsafe.Pointer, rows array, size, offset uintptr)

//go:noescape
func writeValues32bits(values unsafe.Pointer, rows array, size, offset uintptr)

//go:noescpae
func writeValues64bits(values unsafe.Pointer, rows array, size, offset uintptr)

//go:noescape
func writeValues128bits(values unsafe.Pointer, rows array, size, offset uintptr)

func writeValuesBool(values []byte, rows array, size, offset uintptr) {
	writeValuesBitpack(*(*unsafe.Pointer)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesInt32(values []int32, rows array, size, offset uintptr) {
	writeValues32bits(*(*unsafe.Pointer)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesInt64(values []int64, rows array, size, offset uintptr) {
	writeValues64bits(*(*unsafe.Pointer)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesUint32(values []uint32, rows array, size, offset uintptr) {
	writeValues32bits(*(*unsafe.Pointer)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesUint64(values []uint64, rows array, size, offset uintptr) {
	writeValues64bits(*(*unsafe.Pointer)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesUint128(values []byte, rows array, size, offset uintptr) {
	writeValues128bits(*(*unsafe.Pointer)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesFloat32(values []float32, rows array, size, offset uintptr) {
	writeValues32bits(*(*unsafe.Pointer)(unsafe.Pointer(&values)), rows, size, offset)
}

func writeValuesFloat64(values []float64, rows array, size, offset uintptr) {
	writeValues64bits(*(*unsafe.Pointer)(unsafe.Pointer(&values)), rows, size, offset)
}
