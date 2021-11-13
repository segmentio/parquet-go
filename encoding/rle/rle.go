package rle

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type Encoding struct{}

func (e *Encoding) Encoding() format.Encoding {
	return format.RLE
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewDecoder(r)
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoder(w)
}
