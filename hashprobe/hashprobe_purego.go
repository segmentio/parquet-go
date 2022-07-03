//go:build purego || !amd64

package hashprobe

import (
	"unsafe"

	"github.com/segmentio/parquet-go/hashprobe/wyhash"
)

func seed64(table []uint64) uint64 {
	return uint64(uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&table))))
}

func hash64(table []uint64, value uint64) uint64 {
	return wyhash.Sum64Uint64(value, seed64(table))
}

func probe64(table []uint64, len, cap int, keys []uint64, values []int32) int {
	offset := uint64(cap / 64)
	modulo := uint64(cap) - 1

	for i, key := range keys {
		hash := hash64(table, key)

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
