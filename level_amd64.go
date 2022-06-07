//go:build !purego

package parquet

import "golang.org/x/sys/cpu"

//go:noescape
func memsetAVX2(dst []byte, src byte)

func memset(dst []byte, src byte) {
	if len(dst) >= minLenAVX2 && cpu.X86.HasAVX2 {
		memsetAVX2(dst, src)
	} else {
		for i := range dst {
			dst[i] = src
		}
	}
}
