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

func (e *LengthByteArrayEncoding) EncodeByteArray(dst, src []byte) ([]byte, error) {
	page := encoding.ByteArrayPage(src)
	dst = dst[:0]

	if err := page.Validate(); err != nil {
		return dst, err
	}
	offsets := page.Offsets()
	length := getInt32Buffer()
	defer putInt32Buffer(length)

	length.resize(len(offsets) - 1)
	encodeByteArrayLengths(length.values, offsets)

	dst = encodeInt32(dst, length.values)
	dst = append(dst, page.Values()...)
	return dst, nil
}

func (e *LengthByteArrayEncoding) DecodeByteArray(dst, src []byte) ([]byte, error) {
	dst = dst[:0]

	length := getInt32Buffer()
	defer putInt32Buffer(length)

	src, err := length.decode(src)
	if err != nil {
		return dst, encoding.Error(e, err)
	}

	length.values = append(length.values, 0)
	decodeByteArrayLengths(length.values)
	return encoding.EncodeByteArrayPage(dst, length.values, src), nil
}
