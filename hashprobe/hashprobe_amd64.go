//go:build !purego

package hashprobe

//go:noescape
func multiProbe64Uint64(table []uint64, cap, len int, hashes, keys []uint64, values []int32) int
