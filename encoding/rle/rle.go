package rle

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
	return format.RLE
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewDecoderSize(r, e.bufferSize())
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoderSize(w, e.bufferSize())
}

func (e *Encoding) bufferSize() int {
	if e.BufferSize > 0 {
		return e.BufferSize
	}
	return defaultBufferSize
}
