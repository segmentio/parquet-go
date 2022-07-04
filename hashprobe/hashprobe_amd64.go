//go:build !purego

package hashprobe

//go:noescape
func multiProbe64bits(table []uint64, len, cap int, hashes, keys []uint64, values []int32) int
