//go:build !purego

package hashprobe

//go:noescape
func multiProbe32(table []uint32, len, cap int, hashes []uintptr, keys []uint32, values []int32) int

//go:noescape
func multiProbe64(table []uint64, len, cap int, hashes []uintptr, keys []uint64, values []int32) int
