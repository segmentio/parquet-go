//go:build !purego

package wyhash

//go:noescape
func MultiSum32Uint32(hashes, values []uint32, seed uint32)

//go:noescape
func MultiSum64Uint64(hashes, values []uint64, seed uint64)
