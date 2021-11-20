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
	rle := rle.Encoding{BufferSize: e.BufferSize}
	return rle.CanEncode(t)
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return decoder{rle.NewDecoderSize(r, e.bufferSize())}
}

func (e *Encoding) NewEncoder(w io.Writer) encoding.Encoder {
	return encoder{rle.NewEncoderSize(w, e.bufferSize())}
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

type decoder struct{ *rle.Decoder }

func (decoder) Encoding() format.Encoding { return format.RLEDictionary }

type encoder struct{ *rle.Encoder }

func (encoder) Encoding() format.Encoding { return format.RLEDictionary }

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

func (plainDecoder) Encoding() format.Encoding { return format.PlainDictionary }

type plainEncoder struct{ *plain.Encoder }

func (plainEncoder) Encoding() format.Encoding { return format.PlainDictionary }
