package dict

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
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

func (d decoder) Reset(r io.Reader) {
	d.rle.Reset(r)
	d.rle.SetBitWidth(0)
}

func (d decoder) Encoding() format.Encoding { return format.RLEDictionary }

func (d decoder) DecodeInt8(data []int8) (int, error) {
	return d.decode(func() (int, error) { return d.rle.DecodeInt8(data) })
}

func (d decoder) DecodeInt16(data []int16) (int, error) {
	return d.decode(func() (int, error) { return d.rle.DecodeInt16(data) })
}

func (d decoder) DecodeInt32(data []int32) (int, error) {
	return d.decode(func() (int, error) { return d.rle.DecodeInt32(data) })
}

func (d decoder) decode(decode func() (int, error)) (int, error) {
	if d.rle.BitWidth() == 0 {
		bitWidth, err := d.decodeBitWidth()
		if err != nil {
			return 0, err
		}
		d.rle.SetBitWidth(bitWidth)
	}
	return decode()
}

func (d decoder) decodeBitWidth() (int, error) {
	b, err := d.rle.ReadByte()
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, fmt.Errorf("decoding RLE bit width: %w", err)
	}
	if b > 32 {
		return 0, fmt.Errorf("decoding RLE bit width: %d>32", b)
	}
	return int(b), nil
}

type encoder struct {
	encoding.NotImplementedEncoder
	rle *rle.Encoder
}

func (e encoder) Close() error { return e.rle.Close() }

func (e encoder) Reset(w io.Writer) { e.rle.Reset(w) }

func (e encoder) Encoding() format.Encoding { return format.RLEDictionary }

func (e encoder) EncodeInt8(data []int8) error {
	return e.encode(bits.MaxLen8(data), func() error { return e.rle.EncodeInt8(data) })
}

func (e encoder) EncodeInt16(data []int16) error {
	return e.encode(bits.MaxLen16(data), func() error { return e.rle.EncodeInt16(data) })
}

func (e encoder) EncodeInt32(data []int32) error {
	return e.encode(bits.MaxLen32(data), func() error { return e.rle.EncodeInt32(data) })
}

func (e encoder) encode(bitWidth int, encode func() error) error {
	if err := e.encodeBitWidth(bitWidth); err != nil {
		return err
	}
	e.rle.SetBitWidth(bitWidth)
	if err := encode(); err != nil {
		return err
	}
	return e.rle.Close()
}

func (e encoder) encodeBitWidth(bitWidth int) error {
	return e.rle.WriteByte(byte(bitWidth))
}

type plainEncoding struct{ base *Encoding }

func (e plainEncoding) Encoding() format.Encoding {
	return format.PlainDictionary
}

func (e plainEncoding) CanEncode(t format.Type) bool {
	return true
}

func (e plainEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return plainDecoder{plain.NewDecoderSize(r, e.base.bufferSize())}
}

func (e plainEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return plainEncoder{plain.NewEncoder(w)}
}

type plainDecoder struct{ *plain.Decoder }

func (d plainDecoder) Encoding() format.Encoding { return format.PlainDictionary }

type plainEncoder struct{ *plain.Encoder }

func (e plainEncoder) Encoding() format.Encoding { return format.PlainDictionary }
