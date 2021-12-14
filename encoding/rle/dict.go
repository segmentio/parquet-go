package rle

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type DictEncoding struct {
}

func (e *DictEncoding) Encoding() format.Encoding {
	return format.RLEDictionary
}

func (e *DictEncoding) CanEncode(t format.Type) bool {
	return true
}

func (e *DictEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return dictDecoder{rle: NewDecoder(r)}
}

func (e *DictEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return dictEncoder{rle: NewEncoder(w)}
}

func (e *DictEncoding) String() string {
	return "RLE_DICTIONARY"
}

type dictDecoder struct {
	encoding.NotSupportedDecoder
	rle *Decoder
}

func (d dictDecoder) Reset(r io.Reader) {
	d.rle.Reset(r)
	d.rle.SetBitWidth(0)
}

func (d dictDecoder) Encoding() format.Encoding {
	return format.RLEDictionary
}

func (d dictDecoder) DecodeInt32(data []int32) (int, error) {
	if d.rle.BitWidth() == 0 {
		bitWidth, err := d.decodeBitWidth()
		if err != nil {
			return 0, err
		}
		d.rle.SetBitWidth(bitWidth)
	}
	return d.rle.DecodeInt32(data)
}

func (d dictDecoder) decodeBitWidth() (int, error) {
	b, err := d.rle.ReadByte()
	switch err {
	case nil:
		if b > 32 {
			return 0, fmt.Errorf("decoding RLE bit width: %d>32", b)
		}
		return int(b), nil
	case io.EOF:
		return 0, err
	default:
		return 0, fmt.Errorf("decoding RLE bit width: %w", err)
	}
}

type dictEncoder struct {
	encoding.NotSupportedEncoder
	rle *Encoder
}

func (e dictEncoder) Reset(w io.Writer) {
	e.rle.Reset(w)
}

func (e dictEncoder) Encoding() format.Encoding {
	return format.RLEDictionary
}

func (e dictEncoder) EncodeInt32(data []int32) error {
	bitWidth := bits.MaxLen32(data)
	if err := e.encodeBitWidth(bitWidth); err != nil {
		return err
	}
	e.rle.SetBitWidth(bitWidth)
	return e.rle.EncodeInt32(data)
}

func (e dictEncoder) encodeBitWidth(bitWidth int) error {
	return e.rle.WriteByte(byte(bitWidth))
}
