//go:build !purego

package delta

//go:noescape
func decodeByteArrayLengths(offsets []uint32, lengths []int32) (lastOffset uint32, invalidLength int32)
