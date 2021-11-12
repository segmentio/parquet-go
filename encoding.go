package parquet

import (
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
)

var (
	Plain plain.Encoding

	RLE rle.Encoding

	encodings = [10]encoding.Encoding{
		format.Plain: &Plain,
		format.RLE:   &RLE,
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
