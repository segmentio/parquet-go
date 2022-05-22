//go:build !purego

package bytestreamsplit

import (
	"github.com/segmentio/parquet-go/internal/bits"
	"golang.org/x/sys/cpu"
)

var hasAVX512 = cpu.X86.HasAVX512 &&
	cpu.X86.HasAVX512F &&
	cpu.X86.HasAVX512VL

//go:noescape
func encodeFloat(dst, src []byte)

func encodeDouble(dst, src []byte) {
	n := len(src) / 8
	b0 := dst[0*n : 1*n]
	b1 := dst[1*n : 2*n]
	b2 := dst[2*n : 3*n]
	b3 := dst[3*n : 4*n]
	b4 := dst[4*n : 5*n]
	b5 := dst[5*n : 6*n]
	b6 := dst[6*n : 7*n]
	b7 := dst[7*n : 8*n]

	for i, v := range bits.BytesToUint64(src) {
		b0[i] = byte(v >> 0)
		b1[i] = byte(v >> 8)
		b2[i] = byte(v >> 16)
		b3[i] = byte(v >> 24)
		b4[i] = byte(v >> 32)
		b5[i] = byte(v >> 40)
		b6[i] = byte(v >> 48)
		b7[i] = byte(v >> 56)
	}
}

func decodeFloat(dst, src []byte) {
	n := len(src) / 4
	b0 := src[0*n : 1*n]
	b1 := src[1*n : 2*n]
	b2 := src[2*n : 3*n]
	b3 := src[3*n : 4*n]

	dst32 := bits.BytesToUint32(dst)
	for i := range dst32 {
		dst32[i] = uint32(b0[i]) |
			uint32(b1[i])<<8 |
			uint32(b2[i])<<16 |
			uint32(b3[i])<<24
	}
}

func decodeDouble(dst, src []byte) {
	n := len(src) / 8
	b0 := src[0*n : 1*n]
	b1 := src[1*n : 2*n]
	b2 := src[2*n : 3*n]
	b3 := src[3*n : 4*n]
	b4 := src[4*n : 5*n]
	b5 := src[5*n : 6*n]
	b6 := src[6*n : 7*n]
	b7 := src[7*n : 8*n]

	dst64 := bits.BytesToUint64(dst)
	for i := range dst64 {
		dst64[i] = uint64(b0[i]) |
			uint64(b1[i])<<8 |
			uint64(b2[i])<<16 |
			uint64(b3[i])<<24 |
			uint64(b4[i])<<32 |
			uint64(b5[i])<<40 |
			uint64(b6[i])<<48 |
			uint64(b7[i])<<56
	}
}