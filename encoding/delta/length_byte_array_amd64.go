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
func validateLengthValuesDefault(lengths []int32) (sum int, err errno)

//go:noescape
func validateLengthValuesAVX2(lengths []int32) (sum int, err errno)

func validateLengthValues(lengths []int32) (int, error) {
	var sum int
	var err errno
	switch {
	case cpu.X86.HasAVX2:
		sum, err = validateLengthValuesAVX2(lengths)
	default:
		sum, err = validateLengthValuesDefault(lengths)
	}
	return sum, err.check()
}

//go:noescape
func decodeLengthByteArrayAVX2(dst, src []byte, lengths []int32)

func decodeLengthByteArray(dst, src []byte, lengths []int32) ([]byte, error) {
	totalLength, err := validateLengthValues(lengths)
	if err != nil {
		return dst, err
	}
	if totalLength > len(src) {
		return dst, fmt.Errorf("value length is larger than the input size: %d > %d", totalLength, len(src))
	}

	const padding = 64
	size := plain.ByteArrayLengthSize * len(lengths)
	size += totalLength
	src = src[:totalLength]
	dst = resizeNoMemclr(dst, size+padding)

	i := 0
	j := 0
	k := 0
	n := 0

	// To leverage the SEE optimized implementation of the function we must
	// create enough padding at the end to prevent the opportunisitic reads
	// and writes from overflowing past the buffers limits.
	if cpu.X86.HasAVX2 && len(src) > padding {
		k = len(lengths)

		for k > 0 && n < padding {
			k--
			n += int(lengths[k])
		}

		if k > 0 && n >= padding {
			decodeLengthByteArrayAVX2(dst, src, lengths[:k])
			j = len(src) - n
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

	return dst[:size], nil
}
