//go:build purego || !amd64

package hashprobe

func multiProbe64Uint64(table []uint64, len, cap int, hashes, keys []uint64, values []int32) int {
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
