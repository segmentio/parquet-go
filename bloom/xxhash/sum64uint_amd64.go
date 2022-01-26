//go:build !purego

package xxhash

//go:noescape
func MultiSum64Uint64(h []uint64, v []uint64) int
