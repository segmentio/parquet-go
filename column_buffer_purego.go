//go:build !amd64 || purego

package parquet

import "unsafe"

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
	for i, j := 0, 0; i < rows.len; i += 8 {
		b0 := *(*byte)(rows.index(i+0, size, offset))
		b1 := *(*byte)(rows.index(i+1, size, offset))
		b2 := *(*byte)(rows.index(i+2, size, offset))
		b3 := *(*byte)(rows.index(i+3, size, offset))
		b4 := *(*byte)(rows.index(i+4, size, offset))
		b5 := *(*byte)(rows.index(i+5, size, offset))
		b6 := *(*byte)(rows.index(i+6, size, offset))
		b7 := *(*byte)(rows.index(i+7, size, offset))

		values[j] = (b0 & 1) |
			((b1 & 1) << 1) |
			((b2 & 1) << 2) |
			((b3 & 1) << 3) |
			((b4 & 1) << 4) |
			((b5 & 1) << 5) |
			((b6 & 1) << 6) |
			((b7 & 1) << 7)
		j++
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
		values[i] = *(*uint64)(rows.index(i, size, offset))
	}
}

func writeValuesUint128(values []byte, rows array, size, offset uintptr) {
	data := unsafe.Pointer(&values[0])
	for i := 0; i < rows.len; i++ {
		p := rows.index(i, size, offset)
		*(*[16]byte)(unsafe.Add(data, i*16)) = *(*[16]byte)(p)
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
