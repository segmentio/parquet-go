//go:build purego || !amd64

package hashprobe

import (
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func multiProbe32(table []table32Group, numKeys int, hashes []uintptr, keys []uint32, values []int32) int {
	return multiProbe32Default(table, numKeys, hashes, keys, values)
}

func multiProbe64(table []byte, len, cap int, hashes []uintptr, keys []uint64, values []int32) int {
	offset := uintptr(cap) / 8
	modulo := uintptr(cap) - 1

	valuesOffset := offset + 8*uintptr(cap)
	tableFlags := unsafecast.BytesToUint64(table[:offset])
	tableKeys := unsafecast.BytesToUint64(table[offset:valuesOffset])
	tableValues := unsafecast.BytesToInt32(table[valuesOffset:])

	for i, hash := range hashes {
		for {
			hash &= modulo
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
		for {
			hash &= modulo
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

			hash++
		}
	}

	return len
}
