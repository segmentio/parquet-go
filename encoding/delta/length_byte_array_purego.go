//go:build purego || !amd64

package delta

func encodeByteArrayLengths(length, offset []int32) {
	for i := range length {
		length[i] = offset[i+1] - offset[i]
	}
}

func decodeByteArrayLengths(length []int32) {
	offset := int32(0)

	for i, n := range length {
		length[i] = offset
		offset += n
	}
}
