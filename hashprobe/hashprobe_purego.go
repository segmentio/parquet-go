//go:build purego || !amd64

package hashprobe

import (
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

func multiProbe32(table []table32Group, numKeys int, hashes []uintptr, keys []uint32, values []int32) int {
	modulo := uintptr(len(table)) - 1

	for i, hash := range hashes {
		key := keys[i]
		for {
			group := &table[hash&modulo]
			index := 7
			value := int32(0)

			switch key {
			case group.keys[0]:
				index = 0
			case group.keys[1]:
				index = 1
			case group.keys[2]:
				index = 2
			case group.keys[3]:
				index = 3
			case group.keys[4]:
				index = 4
			case group.keys[5]:
				index = 5
			case group.keys[6]:
				index = 6
			}

			if n := group.len; index < int(n) {
				value = int32(group.values[index])
			} else {
				if n == 7 {
					hash++
					continue
				}

				value = int32(numKeys)
				group.len++
				group.keys[n] = key
				group.values[n] = uint32(value)
				numKeys++
			}

			values[i] = value
			break
		}
	}

	return numKeys
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
