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

func (e *Encoding) LevelEncoding() encoding.Encoding {
	return levelEncoding{e}
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

type levelEncoding struct{ base *Encoding }

func (e levelEncoding) Encoding() format.Encoding {
	return e.base.Encoding()
}

func (e levelEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return newLevelDecoderSize(r, e.base.bufferSize())
}

func (e levelEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return newLevelEncoderSize(w, e.base.bufferSize())
}
