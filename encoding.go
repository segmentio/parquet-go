package parquet

import (
	"sort"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/dict"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
)

var (
	Plain plain.Encoding

	RLE rle.Encoding

	Dict dict.Encoding

	encodings = [10]encoding.Encoding{
		format.Plain:           &Plain,
		format.PlainDictionary: Dict.PlainEncoding(),
		format.RLE:             &RLE,
		format.RLEDictionary:   &Dict,
	}
)

func lookupEncoding(enc format.Encoding) encoding.Encoding {
	if enc >= 0 && int(enc) < len(encodings) {
		if e := encodings[enc]; e != nil {
			return e
		}
	}
	return encoding.NotImplemented{}
}

func sortEncodings(encodings []encoding.Encoding) {
	if len(encodings) > 1 {
		sort.Slice(encodings, func(i, j int) bool {
			return encodings[i].Encoding() < encodings[j].Encoding()
		})
	}
}

func dedupeSortedEncodings(encodings []encoding.Encoding) []encoding.Encoding {
	if len(encodings) > 1 {
		i := 0

		for _, c := range encodings[1:] {
			if c.Encoding() != encodings[i].Encoding() {
				i++
				encodings[i] = c
			}
		}

		encodings = encodings[:i+1]
	}
	return encodings
}
