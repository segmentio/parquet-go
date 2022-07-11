//go:build !purego

package sparse

import "golang.org/x/sys/cpu"

func gatherBits(dst []byte, src Uint8Array) int {
	if cpu.X86.HasAVX2 {
		return gatherBitsAVX2(dst, src)
	}
	return gatherBitsDefault(dst, src)
}

func gather32(dst []uint32, src Uint32Array) int {
	if cpu.X86.HasAVX2 {
		return gather32AVX2(dst, src)
	}
	return gather32Default(dst, src)
}

func gather64(dst []uint64, src Uint64Array) int {
	if cpu.X86.HasAVX2 {
		return gather64AVX2(dst, src)
	}
	return gather64Default(dst, src)
}

//go:noescape
func gather128(dst [][16]byte, src Uint128Array) int
