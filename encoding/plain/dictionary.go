package plain

import (
	"io"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

type DictionaryEncoding struct {
}

func (e DictionaryEncoding) Encoding() format.Encoding {
	return format.PlainDictionary
}

func (e DictionaryEncoding) CanEncode(t format.Type) bool {
	return true
}

func (e DictionaryEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return dictionaryDecoder{NewDecoder(r)}
}

func (e DictionaryEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return dictionaryEncoder{NewEncoder(w)}
}

func (e DictionaryEncoding) String() string {
	return "DICT_DICTIONARY"
}

type dictionaryDecoder struct{ *Decoder }

func (d dictionaryDecoder) Encoding() format.Encoding { return format.PlainDictionary }

type dictionaryEncoder struct{ *Encoder }

func (e dictionaryEncoder) Encoding() format.Encoding { return format.PlainDictionary }
