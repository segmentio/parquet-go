//go:build !purego

package delta

import (
	"fmt"

	"github.com/segmentio/parquet-go/encoding/plain"
	"golang.org/x/sys/cpu"
)

type errno int

const (
	ok errno = iota
	invalidNegativeValueLength
)

func (e errno) check() error {
	switch e {
	case ok:
		return nil
	case invalidNegativeValueLength:
		return errInvalidNegativeValueLength
	default:
		return fmt.Errorf("BUG: unknown error code: %d", e)
	}
}

//go:noescape
func decodeLengthValuesDefault(lengths []int32) (sum int, err errno)

//go:noescape
func decodeLengthValuesAVX2(lengths []int32) (sum int, err errno)

func decodeLengthValues(lengths []int32) (int, error) {
	var sum int
	var err errno
	switch {
	case cpu.X86.HasAVX2:
		sum, err = decodeLengthValuesAVX2(lengths)
	default:
		sum, err = decodeLengthValuesDefault(lengths)
	}
	return sum, err.check()
}

//go:noescape
func decodeLengthByteArrayAVX2(dst, src []byte, lengths []int32)

func decodeLengthByteArray(dst, src []byte, lengths []int32, totalLength int) {
	i := 0
	j := 0
	k := 0
	n := 0

	// To leverage the SEE optimized implementation of the function we must
	// create enough padding at the end to prevent the opportunisitic reads
	// and writes from overflowing past the buffers limits.
	if cpu.X86.HasAVX2 && totalLength > lengthByteArrayPadding {
		k = len(lengths)

		for k > 0 && n < lengthByteArrayPadding {
			k--
			n += int(lengths[k])
		}

		if k > 0 && n >= lengthByteArrayPadding {
			decodeLengthByteArrayAVX2(dst, src, lengths[:k])
			j = totalLength - n
			i = plain.ByteArrayLengthSize*k + j
		} else {
			k = 0
		}
	}

	for _, n := range lengths[k:] {
		plain.PutByteArrayLength(dst[i:], int(n))
		i += plain.ByteArrayLengthSize
		i += copy(dst[i:], src[j:j+int(n)])
		j += int(n)
	}
}
