package bits

// Copy copies count bits from src to dst, starting at the bit shift in the
// first byte of src, returning the number of bits written.
//
// shift+count may be beyond the end of src, the Copy function will stop after
// copying all the bits available in the source buffer.
//
// The source and destination buffers must not overlap.
func Copy(dst []byte, src []byte, shift, count uint) int {
	index, shift := IndexShift8(shift)
	if index > 0 {
		if index > uint(len(src)) {
			return 0
		}
		src = src[index:]
	}

	dstBits := BitCount(len(dst))
	srcBits := BitCount(len(src))
	if dstBits < count {
		count = dstBits
	}
	if srcBits < (count + shift) {
		count = srcBits - shift
	}

	dst = dst[:ByteCount(count)]
	src = src[:ByteCount(count+shift)]

	if len(dst) == 0 || len(src) == 0 {
		return 0
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
	dst[i] |= (value & mask) << j

	if (j + count) > 8 {
		mask := byte((1 << ((j + count) % 8)) - 1)
		dst[i+1] |= (value >> (8 - j)) & mask
	}
}
