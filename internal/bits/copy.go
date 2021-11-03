package bits

func Copy(dst []byte, src []byte, shift, count uint) int {
	if len(dst) == 0 || len(src) == 0 {
		return 0
	}

	maxBitsInDst := 8 * uint(len(dst))
	maxBitsInSrc := 8 * uint(len(src))
	if count > maxBitsInDst {
		count = maxBitsInDst
	}
	if count > maxBitsInSrc {
		count = maxBitsInSrc
	}

	for i, n := 0, int(count); n > 0; {
		c := uint(n)
		if c > 8 {
			c = 8
		}
		v := load(src, shift+uint(i), c)
		store(dst, uint(i), c, v)
		i += 8
		n -= 8
	}

	return int(count)
}

func load(src []byte, shift, count uint) byte {
	i, j := shift/8, shift%8
	mask := byte((1 << count) - 1)
	value := (src[i] >> j) & mask

	if (j + count) > 8 {
		mask := byte((1 << ((j + count) % 8)) - 1)
		value |= (src[i+1] & mask) << (8 - j)
	}

	return value
}

func store(dst []byte, shift, count uint, value byte) {
	i, j := shift/8, shift%8
	mask := byte((1 << count) - 1)
	dst[i] |= (value << j) & mask

	if (j + count) > 8 {
		mask := byte((1 << ((j + count) % 8)) - 1)
		dst[i+1] |= (value >> (8 - j)) & mask
	}
}
