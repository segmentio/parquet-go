package delta

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

const (
	maxLinearSearchPrefixLength = 64 // arbitrary
)

type ByteArrayEncoding struct {
	encoding.NotSupported
}

func (e *ByteArrayEncoding) String() string {
	return "DELTA_BYTE_ARRAY"
}

func (e *ByteArrayEncoding) Encoding() format.Encoding {
	return format.DeltaByteArray
}

func (e *ByteArrayEncoding) EncodeByteArray(dst, src []byte) ([]byte, error) {
	page := encoding.ByteArrayPage(src)
	dst = dst[:0]

	if err := page.Validate(); err != nil {
		return dst, err
	}
	values := page.Values()

	prefix := getInt32Buffer()
	defer putInt32Buffer(prefix)

	length := getInt32Buffer()
	defer putInt32Buffer(length)

	lastValue := ([]byte)(nil)
	offsets := page.Offsets()[1:]
	base := int32(0)

	for _, end := range offsets {
		v := values[base:end:end]
		n := len(v)
		p := 0

		if len(v) <= maxLinearSearchPrefixLength {
			p = linearSearchPrefixLength(lastValue, v)
		} else {
			p = binarySearchPrefixLength(lastValue, v)
		}

		prefix.values = append(prefix.values, int32(p))
		length.values = append(length.values, int32(n-p))
		lastValue = v
		base = end
	}

	dst = encodeInt32(dst, prefix.values)
	dst = encodeInt32(dst, length.values)

	base = 0
	for i, end := range offsets {
		dst = append(dst, values[base+prefix.values[i]:end]...)
		base = end
	}
	return dst, nil
}

func (e *ByteArrayEncoding) EncodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error) {
	// The parquet specs say that this encoding is only supported for BYTE_ARRAY
	// values, but the reference Java implementation appears to support
	// FIXED_LEN_BYTE_ARRAY as well:
	// https://github.com/apache/parquet-mr/blob/5608695f5777de1eb0899d9075ec9411cfdf31d3/parquet-column/src/main/java/org/apache/parquet/column/Encoding.java#L211
	if size < 0 || size > encoding.MaxFixedLenByteArraySize {
		return dst[:0], encoding.Error(e, encoding.ErrInvalidArgument)
	}
	if (len(src) % size) != 0 {
		return dst[:0], encoding.ErrEncodeInvalidInputSize(e, "FIXED_LEN_BYTE_ARRAY", len(src))
	}

	prefix := getInt32Buffer()
	defer putInt32Buffer(prefix)

	length := getInt32Buffer()
	defer putInt32Buffer(length)

	totalSize := 0
	lastValue := ([]byte)(nil)

	for i := size; i <= len(src); i += size {
		v := src[i-size : i : i]
		p := linearSearchPrefixLength(lastValue, v)
		n := size - p
		prefix.values = append(prefix.values, int32(p))
		length.values = append(length.values, int32(n))
		lastValue = v
		totalSize += n
	}

	dst = dst[:0]
	dst = encodeInt32(dst, prefix.values)
	dst = encodeInt32(dst, length.values)
	dst = resize(dst, len(dst)+totalSize)

	b := dst[len(dst)-totalSize:]
	i := 0
	j := 0

	for _, p := range prefix.values {
		j += copy(b[j:], src[i+int(p):i+size])
		i += size
	}

	return dst, nil
}

func (e *ByteArrayEncoding) DecodeByteArray(dst, src []byte) ([]byte, error) {
	dst = dst[:0]

	prefix := getInt32Buffer()
	defer putInt32Buffer(prefix)

	suffix := getInt32Buffer()
	defer putInt32Buffer(suffix)

	var err error
	src, err = prefix.decode(src)
	if err != nil {
		return dst, encoding.Errorf(e, "decoding prefix lengths: %w", err)
	}
	src, err = suffix.decode(src)
	if err != nil {
		return dst, encoding.Errorf(e, "decoding suffix lengths: %w", err)
	}
	if len(prefix.values) != len(suffix.values) {
		return dst, encoding.Error(e, errPrefixAndSuffixLengthMismatch(len(prefix.values), len(suffix.values)))
	}

	prefix.values = append(prefix.values, 0)
	decodeByteArrayPrefixLengths(prefix.values, suffix.values)
	return encoding.EncodeByteArrayPage(dst, prefix.values, src), nil
}

func decodeByteArrayPrefixLengths(prefix, suffix []int32) {
	offset := int32(0)

	for i := range suffix {
		p := prefix[i]
		n := suffix[i]
		prefix[i] = offset
		offset += p
		offset += n
	}

	prefix[len(suffix)] = offset
}

func (e *ByteArrayEncoding) DecodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error) {
	dst = dst[:0]

	if size < 0 || size > encoding.MaxFixedLenByteArraySize {
		return dst, encoding.Error(e, encoding.ErrInvalidArgument)
	}

	prefix := getInt32Buffer()
	defer putInt32Buffer(prefix)

	suffix := getInt32Buffer()
	defer putInt32Buffer(suffix)

	var err error
	src, err = prefix.decode(src)
	if err != nil {
		return dst, fmt.Errorf("decoding prefix lengths: %w", err)
	}
	src, err = suffix.decode(src)
	if err != nil {
		return dst, fmt.Errorf("decoding suffix lengths: %w", err)
	}
	if len(prefix.values) != len(suffix.values) {
		return dst, errPrefixAndSuffixLengthMismatch(len(prefix.values), len(suffix.values))
	}
	return decodeFixedLenByteArray(dst, src, size, prefix.values, suffix.values)
}

func linearSearchPrefixLength(base, data []byte) (n int) {
	for n < len(base) && n < len(data) && base[n] == data[n] {
		n++
	}
	return n
}

func binarySearchPrefixLength(base, data []byte) int {
	n := len(base)
	if n > len(data) {
		n = len(data)
	}
	return sort.Search(n, func(i int) bool {
		return !bytes.Equal(base[:i+1], data[:i+1])
	})
}
