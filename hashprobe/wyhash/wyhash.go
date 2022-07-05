package wyhash

import "math/bits"

const (
	m1 = 0xa0761d6478bd642f
	m2 = 0xe7037ed1a0b428db
	m3 = 0x8ebc6af09c88c6e3
	m4 = 0x589965cc75374cc3
	m5 = 0x1d8e4e27c47d124f
)

func mix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

func Hash32(value uint32, seed uintptr) uintptr {
	return Hash64(uint64(value), seed)
}

func Hash64(value uint64, seed uintptr) uintptr {
	return uintptr(mix(m5^8, mix(value^m2, value^uint64(seed)^m1)))
}
