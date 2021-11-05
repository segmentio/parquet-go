package parquet

import (
	"io"

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

func newDecoder(r io.Reader, typ schema.Type, enc schema.Encoding) encoding.Decoder {
	e := lookupEncoding(enc)
	switch typ {
	case schema.Boolean:
		return e.NewBooleanDecoder(r)
	case schema.Int32:
		return e.NewInt32Decoder(r)
	case schema.Int64:
		return e.NewInt64Decoder(r)
	case schema.Int96:
		return e.NewInt96Decoder(r)
	case schema.Float:
		return e.NewFloatDecoder(r)
	case schema.Double:
		return e.NewDoubleDecoder(r)
	case schema.ByteArray:
		return e.NewByteArrayDecoder(r)
	case schema.FixedLenByteArray:
		return e.NewFixedLenByteArrayDecoder(r)
	default:
		panic("unsupported schema type: " + typ.String())
	}
}
