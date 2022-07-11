//go:build purego || !amd64

package sparse

func gatherBits(dst []byte, src Uint8Array) int {
	return gatherBitsDefault(dst, src)
}

func gather32(dst []uint32, src Uint32Array) int {
	return gather32Default(dst, src)
}

func gather64(dst []uint64, src Uint64Array) int {
	return gather64Default(dst, src)
}

func gather128(dst [][16]byte, src Uint128Array) int {
	return gather128Default(dst, src)
}
