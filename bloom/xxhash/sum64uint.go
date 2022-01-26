package xxhash

func Sum64Uint8(v uint8) uint64 {
	h := prime5 + 1
	h ^= uint64(v) * prime5
	return avalanche(rol11(h) * prime1)
}

func Sum64Uint16(v uint16) uint64 {
	h := prime5 + 2
	h ^= uint64(v&0xFF) * prime5
	h = rol11(h) * prime1
	h ^= uint64(v>>8) * prime5
	h = rol11(h) * prime1
	return avalanche(h)
}

func Sum64Uint32(v uint32) uint64 {
	h := prime5 + 4
	h ^= uint64(v) * prime1
	return avalanche((rol23(h) * prime2) + prime3)
}

func Sum64Uint64(v uint64) uint64 {
	h := prime5 + 8
	h ^= round(0, v)
	return avalanche((rol27(h) * prime1) + prime4)
}

func MultiSum64Uint8(h []uint64, v []uint8) int {
	n := min(len(h), len(v))
	h = h[:n]
	v = v[:n]
	for i := range v {
		h[i] = Sum64Uint8(v[i])
	}
	return n
}

func MultiSum64Uint16(h []uint64, v []uint16) int {
	n := min(len(h), len(v))
	h = h[:n]
	v = v[:n]
	for i := range v {
		h[i] = Sum64Uint16(v[i])
	}
	return n
}

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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
