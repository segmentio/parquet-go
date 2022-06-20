//go:build purego || !amd64

package hashprobe

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"math/bits"
)

const (
	m1 = 0xa0761d6478bd642f
	m2 = 0xe7037ed1a0b428db
	m3 = 0x8ebc6af09c88c6e3
	m4 = 0x589965cc75374cc3
	m5 = 0x1d8e4e27c47d124f
)

var (
	hashkey64 uint64
)

func init() {
	bits := [8]byte{}
	io.ReadFull(rand.Reader, bits[:])
	hashkey64 = binary.LittleEndian.Uint64(bits[:])
	hashkey64 |= 1
}

func hash64(seed, value uint64) uint64 {
	return mix64(m5^8, mix64(value^m2, value^seed^hashkey64^m1))
}

func mix64(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

func probe64(table []uint64, len, cap int, keys []uint64, values []int32) int {
	offset := uint64(cap / 64)
	modulo := uint64(cap) - 1

	for i, key := range keys {
		hash := hash64(0, key)

		for {
			position := hash & modulo
			index := position / 64
			shift := position % 64

			position *= 2
			position += offset

			if (table[index] & (1 << shift)) == 0 {
				table[index] |= 1 << shift
				table[position+0] = key
				table[position+1] = uint64(len)
				values[i] = int32(len)
				len++
				break
			}

			if table[position] == keys[i] {
				values[i] = int32(table[position+1])
				break
			}

			hash++
		}
	}

	return len
}
