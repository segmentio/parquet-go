//go:build purego || !amd64

package delta

func decodeFixedLenByteArray(dst, src []byte, size int, prefix, suffix []int32) ([]byte, error) {
	_ = prefix[:len(suffix)]
	_ = suffix[:len(prefix)]

	var lastValue []byte
	for i := range suffix {
		n := int(suffix[i])
		p := int(prefix[i])
		if n < 0 {
			return dst, errInvalidNegativeValueLength(n)
		}
		if n > len(src) {
			return dst, errValueLengthOutOfBounds(n, len(src))
		}
		if p < 0 {
			return dst, errInvalidNegativePrefixLength(p)
		}
		if p > len(lastValue) {
			return dst, errPrefixLengthOutOfBounds(p, len(lastValue))
		}
		j := len(dst)
		dst = append(dst, lastValue[:p]...)
		dst = append(dst, src[:n]...)
		lastValue = dst[j:]
		src = src[n:]
	}
	return dst, nil
}
