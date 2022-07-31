package delta

import (
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

type LengthByteArrayEncoding struct {
	encoding.NotSupported
}

func (e *LengthByteArrayEncoding) String() string {
	return "DELTA_LENGTH_BYTE_ARRAY"
}

func (e *LengthByteArrayEncoding) Encoding() format.Encoding {
	return format.DeltaLengthByteArray
}

func (e *LengthByteArrayEncoding) EncodeByteArray(dst []byte, src []byte, offsets []uint32) ([]byte, error) {
	length := getInt32Buffer()
	defer putInt32Buffer(length)

	length.resize(len(offsets) - 1)
	encodeByteArrayLengths(length.values, offsets)

	dst = dst[:0]
	dst = encodeInt32(dst, length.values)
	dst = append(dst, src...)
	return dst, nil
}

func (e *LengthByteArrayEncoding) DecodeByteArray(dst []byte, src []byte, offsets []uint32) ([]byte, []uint32, error) {
	dst, offsets = dst[:0], offsets[:0]

	length := getInt32Buffer()
	defer putInt32Buffer(length)

	src, err := length.decode(src)
	if err != nil {
		return dst, offsets, e.wrap(err)
	}

	if err := validateByteArrayLengths(length.values); err != nil {
		return dst, offsets, e.wrap(err)
	}

	if size := len(length.values) + 1; cap(offsets) < size {
		offsets = make([]uint32, size)
	} else {
		offsets = offsets[:size]
	}

	lastOffset := decodeByteArrayLengths(offsets, length.values)
	if int(lastOffset) > len(src) {
		return dst, offsets, e.wrap(errValueLengthOutOfBounds(int(lastOffset), len(src)))
	}

	return append(dst, src[:lastOffset]...), offsets, nil
}

func (e *LengthByteArrayEncoding) wrap(err error) error {
	if err != nil {
		err = encoding.Error(e, err)
	}
	return err
}

func validateByteArrayLengths(lengths []int32) error {
	for _, n := range lengths {
		if n < 0 {
			return errInvalidNegativeValueLength(int(n))
		}
	}
	return nil
}

func encodeByteArrayLengths(lengths []int32, offsets []uint32) {
	for i := range lengths {
		lengths[i] = int32(offsets[i+1] - offsets[i])
	}
}

func decodeByteArrayLengths(offsets []uint32, lengths []int32) uint32 {
	lastOffset := uint32(0)

	for i, n := range lengths {
		offsets[i] = lastOffset
		lastOffset += uint32(n)
	}

	offsets[len(lengths)] = lastOffset
	return lastOffset
}
