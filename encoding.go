package parquet

import (
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/bytestreamsplit"
	"github.com/segmentio/parquet-go/encoding/delta"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/encoding/rle"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
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

	// DeltaLengthByteArray is the delta length byte array parquet encoding.
	DeltaLengthByteArray delta.LengthByteArrayEncoding

	// DeltaByteArray is the delta byte array parquet encoding.
	DeltaByteArray delta.ByteArrayEncoding

	// ByteStreamSplit is an encoding for floating-point data.
	ByteStreamSplit bytestreamsplit.Encoding

	// Table indexing the encodings supported by this package.
	encodings = [...]encoding.Encoding{
		format.Plain:                &Plain,
		format.PlainDictionary:      &PlainDictionary,
		format.RLE:                  &RLE,
		format.RLEDictionary:        &RLEDictionary,
		format.DeltaBinaryPacked:    &DeltaBinaryPacked,
		format.DeltaLengthByteArray: &DeltaLengthByteArray,
		format.DeltaByteArray:       &DeltaByteArray,
		format.ByteStreamSplit:      &ByteStreamSplit,
	}

	// Table indexing RLE encodings for repetition and definition levels of
	// all supported bit widths.
	levelEncodings = [...]rle.Encoding{
		0: {BitWidth: 1},
		1: {BitWidth: 2},
		2: {BitWidth: 3},
		3: {BitWidth: 4},
		4: {BitWidth: 5},
		5: {BitWidth: 6},
		6: {BitWidth: 7},
		7: {BitWidth: 8},
	}
)

func isDictionaryEncoding(encoding encoding.Encoding) bool {
	switch encoding.Encoding() {
	case format.PlainDictionary, format.RLEDictionary:
		return true
	default:
		return false
	}
}

// LookupEncoding returns the parquet encoding associated with the given code.
//
// The function never returns nil. If the encoding is not supported,
// encoding.NotSupported is returned.
func LookupEncoding(enc format.Encoding) encoding.Encoding {
	if enc >= 0 && int(enc) < len(encodings) {
		if e := encodings[enc]; e != nil {
			return e
		}
	}
	return encoding.NotSupported{}
}

func lookupLevelEncoding(enc format.Encoding, max int8) encoding.Encoding {
	switch enc {
	case format.RLE:
		return &levelEncodings[bits.Len8(max)-1]
	default:
		return encoding.NotSupported{}
	}
}

func canEncode(e encoding.Encoding, k Kind) bool {
	if isDictionaryEncoding(e) {
		return true
	}
	switch k {
	case Boolean:
		return encoding.CanEncodeBoolean(e)
	case Int32:
		return encoding.CanEncodeInt32(e)
	case Int64:
		return encoding.CanEncodeInt64(e)
	case Int96:
		return encoding.CanEncodeInt96(e)
	case Float:
		return encoding.CanEncodeFloat(e)
	case Double:
		return encoding.CanEncodeDouble(e)
	case ByteArray:
		return encoding.CanEncodeByteArray(e)
	case FixedLenByteArray:
		return encoding.CanEncodeFixedLenByteArray(e)
	default:
		return false
	}
}
