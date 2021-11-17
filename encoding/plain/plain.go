package plain

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

const (
	defaultBufferSize = 1024
)

type Encoding struct{}

func (e *Encoding) Encoding() format.Encoding {
	return format.Plain
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewDecoder(r)
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoder(w)
}

func coerceBitWidth(bitWidth int) int {
	if bitWidth <= 32 {
		return 32
	} else {
		return 64
	}
}
