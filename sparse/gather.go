package sparse

import "unsafe"

func GatherInt32(dst []int32, src Int32Array) int {
	return GatherUint32(*(*[]uint32)(unsafe.Pointer(&dst)), src.Uint32Array())
}

func GatherInt64(dst []int64, src Int64Array) int {
	return GatherUint64(*(*[]uint64)(unsafe.Pointer(&dst)), src.Uint64Array())
}

func GatherFloat32(dst []float32, src Float32Array) int {
	return GatherUint32(*(*[]uint32)(unsafe.Pointer(&dst)), src.Uint32Array())
}

func GatherFloat64(dst []float64, src Float64Array) int {
	return GatherUint64(*(*[]uint64)(unsafe.Pointer(&dst)), src.Uint64Array())
}

func GatherBits(dst []byte, src Uint8Array) int { return gatherBits(dst, src) }

func GatherUint32(dst []uint32, src Uint32Array) int { return gather32(dst, src) }

func GatherUint64(dst []uint64, src Uint64Array) int { return gather64(dst, src) }

func GatherUint128(dst [][16]byte, src Uint128Array) int { return gather128(dst, src) }

func gatherBitsDefault(dst []byte, src Uint8Array) int {
	n := min(len(dst)*8, src.Len())
	i := 0

	if k := (n / 8) * 8; k > 0 {
		for j := 0; i < k; j++ {
			b0 := src.Index(i + 0)
			b1 := src.Index(i + 1)
			b2 := src.Index(i + 2)
			b3 := src.Index(i + 3)
			b4 := src.Index(i + 4)
			b5 := src.Index(i + 5)
			b6 := src.Index(i + 6)
			b7 := src.Index(i + 7)

			dst[j] = (b0 & 1) |
				((b1 & 1) << 1) |
				((b2 & 1) << 2) |
				((b3 & 1) << 3) |
				((b4 & 1) << 4) |
				((b5 & 1) << 5) |
				((b6 & 1) << 6) |
				((b7 & 1) << 7)

			i += 8
		}
	}

	for i < n {
		x := i / 8
		y := i % 8
		b := src.Index(i)
		dst[x] = ((b & 1) << y) | (dst[x] & ^(1 << y))
		i++
	}

	return n
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
