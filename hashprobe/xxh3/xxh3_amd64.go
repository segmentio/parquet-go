//go:build !purego

package xxh3

import "golang.org/x/sys/cpu"

//go:noescape
func multiHash32AVX512(hashes []uintptr, values []uint32, seed uintptr)

func MultiHash32(hashes []uintptr, values []uint32, seed uintptr) {
	if len(hashes) >= 8 && cpu.X86.HasAVX512 {
		n := (len(hashes) / 8) * 8
		multiHash32AVX512(hashes[:n:n], values[:n:n], seed)
		hashes = hashes[n:]
		values = values[n:]
	}
	for i := range hashes {
		hashes[i] = Hash32(values[i], seed)
	}
}
