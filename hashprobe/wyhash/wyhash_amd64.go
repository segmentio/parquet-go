//go:build !purego

package wyhash

//go:noescape
func MultiSum64Uint64(hashes, values []uint64, seed uint64)
