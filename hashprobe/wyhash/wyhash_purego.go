//go:build purego || !amd64

package wyhash

func MultiHash32(hashes []uintptr, values []uint32, seed uintptr) {
	for i, value := range values {
		hashes[i] = Hash32(value, seed)
	}
}

func MultiHash64(hashes []uintptr, values []uint64, seed uintptr) {
	for i, value := range values {
		hashes[i] = Hash64(value, seed)
	}
}
