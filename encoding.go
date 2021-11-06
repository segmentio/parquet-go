package parquet

import (
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/schema"
)

var encodings = [10]encoding.Encoding{
	schema.Plain: new(plain.Encoding),
	schema.RLE:   new(rle.Encoding),
}

func lookupEncoding(enc schema.Encoding) encoding.Encoding {
	if enc >= 0 && int(enc) < len(encodings) {
		if e := encodings[enc]; e != nil {
			return e
		}
	}
	return encoding.NotImplemented{}
}
