package rle

import (
	"io"

	"github.com/segmentio/parquet/encoding"
)

type Encoding struct {
	encoding.NotImplemented
}

func (e *Encoding) NewBooleanDecoder(r io.Reader) encoding.BooleanDecoder {
	return newDecoder(r)
}

func (e *Encoding) NewBooleanEncoder(w io.Writer) encoding.BooleanEncoder {
	return &booleanEncoder{w: w}
}

func (e *Encoding) NewInt32Decoder(r io.Reader) encoding.Int32Decoder {
	return newDecoder(r)
}

func (e *Encoding) NewInt32Encoder(w io.Writer) encoding.Int32Encoder {
	return &intEncoder{w: w}
}

func (e *Encoding) NewInt64Decoder(r io.Reader) encoding.Int64Decoder {
	return newDecoder(r)
}

func (e *Encoding) NewInt64Encoder(w io.Writer) encoding.Int64Encoder {
	return &intEncoder{w: w}
}

func (e *Encoding) NewInt96Decoder(r io.Reader) encoding.Int96Decoder {
	return newDecoder(r)
}

func (e *Encoding) NewInt96Encoder(w io.Writer) encoding.Int96Encoder {
	return &intEncoder{w: w}
}
