package bits

// Unpack copies words of size srcWidth (in bits) from the src buffer to words
// of size dstWidth (in bits) in the dst buffer, returning the number of words
// that were unpacked.
//
// When dstWidth is greater than srcWidth, the upper bits of the destination
// word are set to zero.
//
// The function always writes full bytes to dst, if the last word written does
// not end on a byte boundary, the remaining bits are set to zero.
//
// The behavior is undefined if srcWidth is greater than dstWidth.
//
// The source and destination buffers must not overlap.
func Unpack(dst []byte, dstWidth uint, src []byte, srcWidth uint) int {
	srcWords := BitCount(len(src)) / srcWidth
	dstWords := BitCount(len(dst)) / dstWidth
	wordCount := srcWords
	if dstWidth < srcWidth {
		wordCount = dstWords
	}
	if wordCount == 0 {
		return 0
	}
	src = src[:ByteCount(wordCount*srcWidth)]
	dst = dst[:ByteCount(wordCount*dstWidth)]

	for i := range dst {
		dst[i] = 0
	}

	si := uint(0)
	di := uint(0)

	steps := 1
	srcBitsPerStep := srcWidth
	if srcBitsPerStep > 8 {
		srcBitsPerStep = 8
		steps = ByteCount(srcWidth)
	}

	for n := wordCount; n != 0; n-- {
		nextDstBitIndex := di + dstWidth

		for i := steps; i != 0; i-- {
			v := load(src, si, srcBitsPerStep)
			store(dst, di, srcBitsPerStep, v)
			si += srcBitsPerStep
			di += srcBitsPerStep
		}

		di = nextDstBitIndex
	}

	return int(wordCount)
}
