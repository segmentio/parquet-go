//go:build !purego

package hashprobe

//go:noescape
func multiProbe32bits(table []uint32, len, cap int, hashes, keys []uint32, values []int32) int

//go:noescape
func multiProbe64bits(table []uint64, len, cap int, hashes, keys []uint64, values []int32) int
