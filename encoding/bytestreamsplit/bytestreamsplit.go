package bytestreamsplit

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type Encoding struct{}

func (e *Encoding) Encoding() format.Encoding {
	return format.ByteStreamSplit
}

func (e *Encoding) CanEncode(t format.Type) bool {
	return t == format.Float || t == format.Double
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoder(w)
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewDecorder(r)
}

func (e *Encoding) String() string {
	return "BYTE_STREAM_SPLIT"
}
