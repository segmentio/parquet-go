//go:build purego || !amd64

package xxhash

func MultiSum64Uint32(h []uint64, v []uint32) int {
	n := min(len(h), len(v))
	h = h[:n]
	v = v[:n]
	for i := range v {
		h[i] = Sum64Uint32(v[i])
	}
	return n
}

func MultiSum64Uint64(h []uint64, v []uint64) int {
	n := min(len(h), len(v))
	h = h[:n]
	v = v[:n]
	for i := range v {
		h[i] = Sum64Uint64(v[i])
	}
	return n
}
