//go:build !purego

package delta

import (
	"github.com/segmentio/parquet-go/encoding/plain"
)

//go:noescape
func validatePrefixAndSuffixLengthValuesAVX2(prefix, suffix []int32, maxLength int) (totalPrefixLength, totalSuffixLength int, ok bool)

func validatePrefixAndSuffixLengthValues(prefix, suffix []int32, maxLength int) (totalPrefixLength, totalSuffixLength int, err error) {
	lastValueLength := 0

	for i := range prefix {
		p := int(prefix[i])
		n := int(suffix[i])
		if p < 0 {
			err = errInvalidNegativePrefixLength(p)
			return
		}
		if n < 0 {
			err = errInvalidNegativeValueLength(n)
			return
		}
		if p > lastValueLength {
			err = errPrefixLengthOutOfBounds(p, lastValueLength)
			return
		}
		totalPrefixLength += p
		totalSuffixLength += n
		lastValueLength = p + n
	}

	if totalSuffixLength > maxLength {
		err = errValueLengthOutOfBounds(totalSuffixLength, maxLength)
		return
	}

	return totalPrefixLength, totalSuffixLength, nil
}

func decodeByteArray(dst, src []byte, prefix, suffix []int32) ([]byte, error) {
	totalPrefixLength, totalSuffixLength, err := validatePrefixAndSuffixLengthValues(prefix, suffix, len(src))
	if err != nil {
		return dst, err
	}

	totalLength := plain.ByteArrayLengthSize*len(prefix) + totalPrefixLength + totalSuffixLength
	dst = resizeNoMemclr(dst, totalLength+padding)

	_ = prefix[:len(suffix)]
	_ = suffix[:len(prefix)]

	var lastValue []byte
	var i int
	var j int

	for k := range prefix {
		p := int(prefix[k])
		n := int(suffix[k])
		plain.PutByteArrayLength(dst[i:], p+n)
		i += plain.ByteArrayLengthSize
		k := i
		i += copy(dst[i:], lastValue[:p])
		i += copy(dst[i:], src[j:j+n])
		j += n
		lastValue = dst[k:]
	}

	return dst[:totalLength], nil
}

func decodeFixedLenByteArray(dst, src []byte, prefix, suffix []int32) ([]byte, error) {
	totalPrefixLength, totalSuffixLength, err := validatePrefixAndSuffixLengthValues(prefix, suffix, len(src))
	if err != nil {
		return dst, err
	}

	totalLength := totalPrefixLength + totalSuffixLength
	dst = resizeNoMemclr(dst, totalLength+padding)

	_ = prefix[:len(suffix)]
	_ = suffix[:len(prefix)]

	var lastValue []byte
	var i int
	var j int

	for k := range prefix {
		p := int(prefix[k])
		n := int(suffix[k])
		k := i
		i += copy(dst[i:], lastValue[:p])
		i += copy(dst[i:], src[j:j+n])
		j += n
		lastValue = dst[k:]
	}

	return dst[:totalLength], nil
}
