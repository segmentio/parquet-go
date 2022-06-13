//go:build purego || !amd64

package delta

import (
	"fmt"

	"github.com/segmentio/parquet-go/encoding/plain"
)

func decodeLengthByteArray(dst, src []byte, lengths []int32) ([]byte, error) {
	for _, n := range lengths {
		if int(n) < 0 {
			return dst, fmt.Errorf("invalid negative value length: %d", n)
		}
		if int(n) > len(src) {
			return dst, fmt.Errorf("value length is larger than the input size: %d > %d", n, len(src))
		}
		dst = plain.AppendByteArray(dst, src[:n])
		src = src[n:]
	}
	return dst, nil
}
