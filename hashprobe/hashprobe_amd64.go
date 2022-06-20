//go:build !purego

package hashprobe

//go:noescape
func hash64(seed, value uint64) uint64

//go:noescape
func probe64(table []uint64, len, cap int, keys []uint64, values []int32) int
