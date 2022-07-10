//go:build purego || !amd64

package xxh3

func MultiHash32(hashes []uintptr, values []uint32, seed uintptr) {
	for i := range hashes {
		hashes[i] = Hash32(values[i], seed)
	}
}
