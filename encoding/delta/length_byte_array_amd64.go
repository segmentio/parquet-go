//go:build !purego

package delta

import "golang.org/x/sys/cpu"

//go:noescape
func encodeByteArrayLengthsDefault(length, offset []int32)

//go:noescape
func encodeByteArrayLengthsAVX2(length, offset []int32)

func encodeByteArrayLengths(length, offset []int32) {
	if cpu.X86.HasAVX2 {
		encodeByteArrayLengthsAVX2(length, offset)
	} else {
		encodeByteArrayLengthsDefault(length, offset)
	}
}

//go:noescape
func decodeByteArrayLengths(length []int32)
