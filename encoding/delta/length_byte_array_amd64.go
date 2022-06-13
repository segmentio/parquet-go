//go:build !purego

package delta

import (
	"github.com/segmentio/parquet-go/encoding/plain"
	"golang.org/x/sys/cpu"
)

//go:noescape
func validateLengthValuesAVX2(lengths []int32) (sum int, err errno)

func validateLengthValues(lengths []int32, maxLength int) (totalLength int, err error) {
	if cpu.X86.HasAVX2 {
		var errno errno
		totalLength, errno = validateLengthValuesAVX2(lengths)
		switch errno {
		case ok:
		case invalidNegativeValueLength:
			err = errInvalidNegativeValueLength(findNegativeLength(lengths))
		default:
			err = errUnknownErrorCode(errno)
		}
	} else {
		for i := range lengths {
			n := int(lengths[i])
			if n < 0 {
				return 0, errInvalidNegativeValueLength(n)
			}
			if n > maxLength {
				return 0, errValueLengthOutOfBounds(n, maxLength)
			}
			totalLength += n
		}
	}
	if totalLength > maxLength {
		err = errValueLengthOutOfBounds(totalLength, maxLength)
	}
	return totalLength, err
}

//go:noescape
func decodeLengthByteArrayAVX2(dst, src []byte, lengths []int32)

func decodeLengthByteArray(dst, src []byte, lengths []int32) ([]byte, error) {
	totalLength, err := validateLengthValues(lengths, len(src))
	if err != nil {
		return dst, err
	}

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
