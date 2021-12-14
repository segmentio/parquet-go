package parquet

import (
	"sort"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/bytestreamsplit"
	"github.com/segmentio/parquet/encoding/delta"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
)

var (
	// Plain is the default parquet encoding.
	Plain plain.Encoding

	// RLE is the hybrid bit-pack/run-length parquet encoding.
	RLE rle.Encoding

	// PlainDictionary is the plain dictionary parquet encoding.
	//
	// This encoding should not be used anymore in parquet 2.0 and later,
	// it is implemented for backwards compatibility to support reading
	// files that were encoded with older parquet libraries.
	PlainDictionary plain.DictionaryEncoding

	// RLEDictionary is the RLE dictionary parquet encoding.
	RLEDictionary rle.DictionaryEncoding

	// DeltaBinaryPacked is the delta binary packed parquet encoding.
	DeltaBinaryPacked delta.BinaryPackedEncoding

	// ByteStreamSplit is an encoding for floating-point data.
	ByteStreamSplit bytestreamsplit.Encoding

	// Table indexing the encodings supported by this package.
	encodings = [...]encoding.Encoding{
		format.Plain:             &Plain,
		format.PlainDictionary:   &PlainDictionary,
		format.RLE:               &RLE,
		format.RLEDictionary:     &RLEDictionary,
		format.DeltaBinaryPacked: &DeltaBinaryPacked,
		format.ByteStreamSplit:   &ByteStreamSplit,
	}
)

func lookupEncoding(enc format.Encoding) encoding.Encoding {
	if enc >= 0 && int(enc) < len(encodings) {
		if e := encodings[enc]; e != nil {
			return e
		}
	}
	return encoding.NotSupported{}
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
