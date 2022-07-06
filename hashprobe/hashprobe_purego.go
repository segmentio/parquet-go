//go:build purego || !amd64

package hashprobe

import (
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func multiProbe32(table []uint32, len, cap int, hashes []uintptr, keys []uint32, values []int32) int {
	offset := uintptr(cap) / 32
	modulo := uintptr(cap) - 1

	for i, hash := range hashes {
		for {
			position := hash & modulo
			index := position / 32
			shift := position % 32

			position *= 2
			position += offset

			if (table[index] & (1 << shift)) == 0 {
				table[index] |= 1 << shift
				table[position+0] = keys[i]
				table[position+1] = uint32(len)
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

func multiProbe64(table []uint64, len, cap int, hashes []uintptr, keys []uint64, values []int32) int {
	offset := uintptr(cap) / 64
	modulo := uintptr(cap) - 1

	for i, hash := range hashes {
		for {
			position := hash & modulo
			index := position / 64
			shift := position % 64

			position *= 2
			position += offset

			if (table[index] & (1 << shift)) == 0 {
				table[index] |= 1 << shift
				table[position+0] = keys[i]
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

func multiProbe128(table []byte, len, cap int, hashes []uintptr, keys [][16]byte, values []int32) int {
	offset := uintptr(cap) / 8
	modulo := uintptr(cap) - 1

	valuesOffset := offset + 16*uintptr(cap)
	tableFlags := unsafecast.BytesToUint64(table[:offset])
	tableKeys := unsafecast.BytesToUint128(table[offset:valuesOffset])
	tableValues := unsafecast.BytesToInt32(table[valuesOffset:])

	for i, hash := range hashes {
		hash &= modulo

		for {
			index := hash / 64
			shift := hash % 64

			if (tableFlags[index] & (1 << shift)) == 0 {
				tableFlags[index] |= 1 << shift
				tableKeys[hash] = keys[i]
				tableValues[hash] = int32(len)
				values[i] = int32(len)
				len++
				break
			}

			if tableKeys[hash] == keys[i] {
				values[i] = tableValues[hash]
				break
			}

			hash = (hash + 1) & modulo
		}
	}

	return len
}
