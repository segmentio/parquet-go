package bits

// Pack copies words of size srcWidth (in bits) from the src buffer to words
// of size dstWidth (in bits) in the dst buffer, returning the number of words
// that were packed.
//
// When dstWidth is greater than srcWidth, the upper bits of the destination
// word are set to zero.
//
// When srcWidth is greater than dstWith, the upper bits of the source are
// discarded.
//
// The function always writes full bytes to dst, if the last word written does
// not end on a byte boundary, the remaining bits are set to zero.
//
// The source and destination buffers must not overlap.
func Pack(dst []byte, dstWidth uint, src []byte, srcWidth uint) int {
	srcWords := BitCount(len(src)) / srcWidth
	dstWords := BitCount(len(dst)) / dstWidth
	wordCount := srcWords
	if dstWords < srcWords {
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
	if dstWidth < srcWidth {
		srcBitsPerStep = dstWidth
	}
	if srcBitsPerStep > 8 {
		steps = ByteCount(srcBitsPerStep)
		srcBitsPerStep = 8
	}

	for n := wordCount; n != 0; n-- {
		nextSrcBitIndex := si + srcWidth
		nextDstBitIndex := di + dstWidth

		for i := steps; i != 0; i-- {
			v := load(src, si, srcBitsPerStep)
			store(dst, di, srcBitsPerStep, v)
			si += srcBitsPerStep
			di += srcBitsPerStep
		}

		si = nextSrcBitIndex
		di = nextDstBitIndex
	}

	return int(wordCount)
}
