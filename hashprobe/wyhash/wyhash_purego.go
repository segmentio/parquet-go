//go:build purego || !amd64

package wyhash

func MultiSum32Uint32(hashes, values []uint32, seed uint32) {
	for i, value := range values {
		hashes[i] = Sum32Uint32(value, seed)
	}
}

func MultiSum64Uint64(hashes, values []uint64, seed uint64) {
	for i, value := range values {
		hashes[i] = Sum64Uint64(value, seed)
	}
}
