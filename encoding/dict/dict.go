package dict

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
)

type Encoding struct {
	BufferSize int
}

func (e *Encoding) Encoding() format.Encoding {
	return format.RLEDictionary
}

func (e *Encoding) CanEncode(t format.Type) bool {
	return true
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return decoder{rle: rle.NewDecoderSize(r, e.bufferSize())}
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return encoder{rle: rle.NewEncoderSize(w, e.bufferSize())}
}

func (e *Encoding) PlainEncoding() encoding.Encoding {
	return plainEncoding{base: e}
}

func (e *Encoding) bufferSize() int {
	if e.BufferSize > 0 {
		return e.BufferSize
	}
	return encoding.DefaultBufferSize
}

type decoder struct {
	encoding.NotImplementedDecoder
	rle *rle.Decoder
}

func (d decoder) Close() error { return d.rle.Close() }

func (d decoder) Reset(r io.Reader) { d.rle.Reset(r) }

func (d decoder) Encoding() format.Encoding { return format.RLEDictionary }

func (d decoder) DecodeIntArray(values encoding.IntArrayBuffer) error {
	bitWidth, err := d.rle.DecodeBitWidth()
	if err != nil {
		return err
	}
	d.rle.SetBitWidth(bitWidth)
	return d.rle.DecodeIntArray(values)
}

type encoder struct {
	encoding.NotImplementedEncoder
	rle *rle.Encoder
}

func (e encoder) Close() error { return e.rle.Close() }

func (e encoder) Reset(w io.Writer) { e.rle.Reset(w) }

func (e encoder) Encoding() format.Encoding { return format.RLEDictionary }

func (e encoder) EncodeIntArray(values encoding.IntArrayView) error {
	bitWidth := values.BitWidth()
	e.rle.SetBitWidth(bitWidth)
	e.rle.EncodeBitWidth(bitWidth)
	return e.rle.EncodeIntArray(values)
}

type plainEncoding struct{ base *Encoding }

func (e plainEncoding) Encoding() format.Encoding {
	return format.PlainDictionary
}

func (e plainEncoding) CanEncode(t format.Type) bool {
	return true
}

func (e plainEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return plainDecoder{plain: plain.NewDecoderSize(r, e.base.bufferSize())}
}

func (e plainEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return plainEncoder{plain: plain.NewEncoder(w)}
}

type plainDecoder struct {
	encoding.NotImplementedDecoder
	plain *plain.Decoder
}

func (d plainDecoder) Close() error { return d.plain.Close() }

func (d plainDecoder) Reset(r io.Reader) { d.plain.Reset(r) }

func (d plainDecoder) Encoding() format.Encoding { return format.PlainDictionary }

func (d plainDecoder) DecodeIntArray(values encoding.IntArrayBuffer) error {
	d.plain.SetBitWidth(32)
	return d.plain.DecodeIntArray(values)
}

type plainEncoder struct {
	encoding.NotImplementedEncoder
	plain *plain.Encoder
}

func (e plainEncoder) Close() error { return e.plain.Close() }

func (e plainEncoder) Reset(w io.Writer) { e.plain.Reset(w) }

func (e plainEncoder) Encoding() format.Encoding { return format.PlainDictionary }

func (e plainEncoder) EncodeIntArray(values encoding.IntArrayView) error {
	e.plain.SetBitWidth(32)
	return e.plain.EncodeIntArray(values)
}
