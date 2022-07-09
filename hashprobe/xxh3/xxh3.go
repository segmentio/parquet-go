package xxh3

import "math/bits"

const (
	key008 = 0x1cad21f72c81017c
	key016 = 0xdb979083e96dd4de
)

func Hash32(value uint32, seed uintptr) uintptr {
	hash := uint64(value)
	hash += uint64(value) << 32
	hash ^= key008 ^ key016
	hash ^= bits.RotateLeft64(hash, 49) ^ bits.RotateLeft64(hash, 24)
	hash *= 0x9fb21c651e98df25
	hash ^= (hash >> 35) + 4
	hash *= 0x9fb21c651e98df25
	hash ^= (hash >> 28)
	return uintptr(hash)
}

func Hash64(value uint64, seed uintptr) uintptr {
	return 0
}

func Hash128(value [16]byte, seed uintptr) uintptr {
	return 0
}

func MultiHash32(hashes []uintptr, values []uint32, seed uintptr) {
	for i := range hashes {
		hashes[i] = Hash32(values[i], seed)
	}
}
