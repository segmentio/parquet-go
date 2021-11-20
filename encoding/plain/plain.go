package plain

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

const (
	defaultBufferSize = 1024
)

type Encoding struct {
	BufferSize int
}

func (e *Encoding) Encoding() format.Encoding {
	return format.Plain
}

func (e *Encoding) CanEncode(format.Type) bool {
	return true
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewDecoderSize(r, e.bufferSize())
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoder(w)
}

func (e *Encoding) bufferSize() int {
	if e.BufferSize > 0 {
		return e.BufferSize
	}
	return defaultBufferSize
}

func coerceBitWidth(bitWidth int) int {
	if bitWidth <= 32 {
		return 32
	} else {
		return 64
	}
}
