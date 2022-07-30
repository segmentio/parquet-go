package bytestreamsplit

import (
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

// This encoder implements a version of the Byte Stream Split encoding as described
// in https://github.com/apache/parquet-format/blob/master/Encodings.md#byte-stream-split-byte_stream_split--9
type Encoding struct {
	encoding.NotSupported
}

func (e *Encoding) String() string {
	return "BYTE_STREAM_SPLIT"
}

func (e *Encoding) Encoding() format.Encoding {
	return format.ByteStreamSplit
}

func (e *Encoding) EncodeFloat(dst []byte, src encoding.Values) ([]byte, error) {
	buf := src.Bytes(encoding.Float)
	dst = resize(dst, len(buf))
	encodeFloat(dst, buf)
	return dst, nil
}

func (e *Encoding) EncodeDouble(dst []byte, src encoding.Values) ([]byte, error) {
	buf := src.Bytes(encoding.Double)
	dst = resize(dst, len(buf))
	encodeDouble(dst, buf)
	return dst, nil
}

func (e *Encoding) DecodeFloat(dst encoding.Values, src []byte) (encoding.Values, error) {
	if (len(src) % 4) != 0 {
		return dst, encoding.ErrDecodeInvalidInputSize(e, "FLOAT", len(src))
	}
	buf := resize(dst.Bytes(encoding.Float), len(src))
	decodeFloat(buf, src)
	return encoding.FloatValuesFromBytes(buf), nil
}

func (e *Encoding) DecodeDouble(dst encoding.Values, src []byte) (encoding.Values, error) {
	if (len(src) % 8) != 0 {
		return dst, encoding.ErrDecodeInvalidInputSize(e, "DOUBLE", len(src))
	}
	buf := resize(dst.Bytes(encoding.Double), len(src))
	decodeDouble(buf, src)
	return encoding.DoubleValuesFromBytes(buf), nil
}

func resize(buf []byte, size int) []byte {
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	return buf
}
