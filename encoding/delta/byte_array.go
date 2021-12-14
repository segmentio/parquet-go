package delta

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type ByteArrayEncoding struct {
}

func (e *ByteArrayEncoding) Encoding() format.Encoding {
	return format.DeltaByteArray
}

func (e *ByteArrayEncoding) CanEncode(t format.Type) bool {
	return t == format.ByteArray
}

func (e *ByteArrayEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewByteArrayDecoder(r)
}

func (e *ByteArrayEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewByteArrayEncoder(w)
}

func (e *ByteArrayEncoding) String() string {
	return "DELTA_BYTE_ARRAY"
}
