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

func Sum32Uint32(value, seed uint32) uint32 {
	return uint32(mix(m5^8, mix(uint64(value)^m2, uint64(value)^uint64(seed)^m1)))
}

func Sum64Uint64(value, seed uint64) uint64 {
	return mix(m5^8, mix(value^m2, value^seed^m1))
}
