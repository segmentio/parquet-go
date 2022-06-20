package hashprobe

import "math/bits"

const (
	prime1 = 0x9E3779B1 // 0b10011110001101110111100110110001
	prime2 = 0x85EBCA77 // 0b10000101111010111100101001110111
	prime3 = 0xC2B2AE3D // 0b11000010101100101010111000111101
	prime4 = 0x27D4EB2F // 0b00100111110101001110101100101111
	prime5 = 0x165667B1 // 0b00010110010101100110011110110001
)

func xxhash32x32bits(v uint32) uint32 {
	h := uint32(prime5) + 4
	h += v * prime3
	h = bits.RotateLeft32(h, 17) * prime4
	return xxhash32Avalanche(h)
}

func xxhash32x64bits(v uint64) uint32 {
	h := uint32(prime5) + 8
	h += uint32(v) * prime3
	h = bits.RotateLeft32(h, 17) * prime4
	h += uint32(v>>32) * prime3
	h = bits.RotateLeft32(h, 17) * prime4
	return xxhash32Avalanche(h)
}

func xxhash32Avalanche(h uint32) uint32 {
	h ^= h >> 15
	h *= prime2
	h ^= h >> 13
	h *= prime3
	h ^= h >> 16
	return h
}
