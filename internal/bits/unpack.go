package bits

func Unpack(dst []byte, dstWidth uint, src []byte, srcWidth uint) int {
	srcWords := (8 * uint(len(src))) / srcWidth
	dstWords := (8 * uint(len(dst))) / dstWidth
	wordCount := srcWords
	if dstWidth < srcWidth {
		wordCount = dstWords
	}
	if wordCount == 0 {
		return 0
	}
	src = src[:((wordCount*srcWidth)+7)/8]
	dst = dst[:((wordCount*dstWidth)+7)/8]

	for i := range dst {
		dst[i] = 0
	}

	si := uint(0)
	di := uint(0)

	steps := uint(1)
	srcBitsPerStep := srcWidth
	if srcBitsPerStep > 8 {
		srcBitsPerStep = 8
		steps = (srcWidth + 7) / 8
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
