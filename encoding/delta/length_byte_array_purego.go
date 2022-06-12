//go:build purego || !amd64

package delta

import "github.com/segmentio/parquet-go/encoding/plain"

func decodeLengthValues(lengths []int32) (sum int, err error) {
	for _, n := range lengths {
		sum += int(n)
		if n < 0 {
			return sum, errInvalidNegativeValueLength
		}
	}
	return sum, nil
}

func decodeLengthByteArray(dst, src []byte, lengths []int32) {
	i := 0
	j := 0

	for _, n := range lengths {
		plain.PutByteArrayLength(dst[i:], int(n))
		i += plain.ByteArrayLengthSize
		i += copy(dst[i:], src[j:j+int(n)])
		j += int(n)
	}
}
