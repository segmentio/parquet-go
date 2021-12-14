package plain

import (
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

type DictEncoding struct {
}

func (e DictEncoding) Encoding() format.Encoding {
	return format.PlainDictionary
}

func (e DictEncoding) CanEncode(t format.Type) bool {
	return true
}

func (e DictEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return dictDecoder{NewDecoder(r)}
}

func (e DictEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return dictEncoder{NewEncoder(w)}
}

func (e DictEncoding) String() string {
	return "DICT_DICTIONARY"
}

type dictDecoder struct{ *Decoder }

func (d dictDecoder) Encoding() format.Encoding { return format.PlainDictionary }

type dictEncoder struct{ *Encoder }

func (e dictEncoder) Encoding() format.Encoding { return format.PlainDictionary }
