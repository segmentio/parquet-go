package rle

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type Encoding struct {
}

func (e *Encoding) Encoding() format.Encoding {
	return format.RLE
}

func (e *Encoding) CanEncode(t format.Type) bool {
	return t == format.Boolean || t == format.Int32 || t == format.Int64 || t == format.Int96
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewDecoder(r)
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoder(w)
}

func (e *Encoding) String() string {
	return "RLE"
}
