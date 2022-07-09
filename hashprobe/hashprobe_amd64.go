//go:build !purego

package hashprobe

import "golang.org/x/sys/cpu"

var (
	multiProbe32 = multiProbe32Default
)

func init() {
	if cpu.X86.HasAVX2 {
		multiProbe32 = multiProbe32AVX2
	}
}

//go:noescape
func multiProbe32Default(table []table32Group, numKeys int, hashes []uintptr, keys []uint32, values []int32) (int, int)

//go:noescape
func multiProbe32AVX2(table []table32Group, numKeys int, hashes []uintptr, keys []uint32, values []int32) (int, int)

//go:noescape
func multiProbe64(table []byte, len, cap int, hashes []uintptr, keys []uint64, values []int32) int

//go:noescape
func multiProbe128(table []byte, len, cap int, hashes []uintptr, keys [][16]byte, values []int32) int
