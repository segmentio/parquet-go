//go:build !purego

package rle

import "golang.org/x/sys/cpu"

var hasAVX2 = cpu.X86.HasAVX2

//go:noescape
func isZero(data []byte) bool

//go:noescape
func isOnes(data []byte) bool

//go:noescape
func encodeBytesBitpack(dst []byte, src []uint64, bitWidth uint) int
