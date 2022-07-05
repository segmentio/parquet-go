//go:build !amd64 || purego

package parquet

func broadcastValueInt32(dst []int32, src int8) {
	value := 0x01010101 * int32(src)
	for i := range dst {
		dst[i] = value
	}
}

func broadcastRangeInt32(dst []int32, base int32) {
	for i := range dst {
		dst[i] = base + int32(i)
	}
}

func writeValuesBool(values []byte, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*bool)(rows.index(i, size, offset))
	}
}

func writeValuesInt32(values []int32, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*int32)(rows.index(i, size, offset))
	}
}

func writeValuesInt64(values []int64, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*int64)(rows.index(i, size, offset))
	}
}

func writeValuesUint32(values []uint32, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*uint32)(rows.index(i, size, offset))
	}
}

func writeValuesUint64(values []uint64, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*uint32)(rows.index(i, size, offset))
	}
}

func writeValuesFloat32(values []float32, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*float32)(rows.index(i, size, offset))
	}
}

func writeValuesFloat64(values []float64, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*float64)(rows.index(i, size, offset))
	}
}

func writeValuesBE128(values [][16]byte, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*[16]byte)(rows.index(i, size, offset))
	}
}
