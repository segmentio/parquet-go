//go:build purego || !amd64

package sparse

func gatherBits(dst []byte, src Uint8Array) int {
	return gatherBitsDefault(dst, src)
}

func gather32(dst []uint32, src Uint32Array) int {
	n := min(len(dst), src.Len())

	for i := range dst[:n] {
		dst[i] = src.Index(i)
	}

	return n
}

func gather64(dst []uint64, src Uint64Array) int {
	n := min(len(dst), src.Len())

	for i := range dst[:n] {
		dst[i] = src.Index(i)
	}

	return n
}

func gather128(dst [][16]byte, src Uint128Array) int {
	n := min(len(dst), src.Len())

	for i := range dst[:n] {
		dst[i] = src.Index(i)
	}

	return n
}
