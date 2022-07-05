//go:build purego || !amd64

package hashprobe

func multiProbe32bits(table []uint32, len, cap int, hashes, keys []uint32, values []int32) int {
	offset := uint32(cap / 32)
	modulo := uint32(cap) - 1

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

func multiProbe64bits(table []uint64, len, cap int, hashes, keys []uint64, values []int32) int {
	offset := uint64(cap / 64)
	modulo := uint64(cap) - 1

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
