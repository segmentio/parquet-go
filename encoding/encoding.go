// Package encoding provides the generic APIs implemented by parquet encodings
// in its sub-packages.
package encoding

import (
	"math"

	"github.com/segmentio/parquet-go/format"
)

const (
	MaxFixedLenByteArraySize = math.MaxInt16
)

// The Encoding interface is implemented by types representing parquet column
// encodings.
//
// Encoding instances must be safe to use concurrently from multiple goroutines.
type Encoding interface {
	// Returns a human-readable name for the encoding.
	String() string

	// Returns the parquet code representing the encoding.
	Encoding() format.Encoding

	// Encode methods serialize the source sequence of values into the
	// destination buffer, potentially reallocating it if it was too short to
	// contain the output.
	//
	// The methods panic if the type of src values differ from the type of
	// values being encoded.
	EncodeLevels(dst []byte, src Values) ([]byte, error)
	EncodeBoolean(dst []byte, src Values) ([]byte, error)
	EncodeInt32(dst []byte, src Values) ([]byte, error)
	EncodeInt64(dst []byte, src Values) ([]byte, error)
	EncodeInt96(dst []byte, src Values) ([]byte, error)
	EncodeFloat(dst []byte, src Values) ([]byte, error)
	EncodeDouble(dst []byte, src Values) ([]byte, error)
	EncodeByteArray(dst []byte, src Values) ([]byte, error)
	EncodeFixedLenByteArray(dst []byte, src Values) ([]byte, error)

	// Decode methods deserialize from the source buffer into the destination
	// slice, potentially growing it if it was too short to contain the result.
	//
	// The methods panic if the type of dst values differ from the type of
	// values being decoded.
	DecodeLevels(dst Values, src []byte) (Values, error)
	DecodeBoolean(dst Values, src []byte) (Values, error)
	DecodeInt32(dst Values, src []byte) (Values, error)
	DecodeInt64(dst Values, src []byte) (Values, error)
	DecodeInt96(dst Values, src []byte) (Values, error)
	DecodeFloat(dst Values, src []byte) (Values, error)
	DecodeDouble(dst Values, src []byte) (Values, error)
	DecodeByteArray(dst Values, src []byte) (Values, error)
	DecodeFixedLenByteArray(dst Values, src []byte) (Values, error)
}
