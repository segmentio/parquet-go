// Package encoding provides the generic APIs implemented by parquet encodings
// in its sub-packages.
package encoding

import (
	"math"

	"github.com/segmentio/parquet-go/deprecated"
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
	// When encoding columns of byte array values, the input is expected to be
	// formatted with the PLAIN encoding (a sequence of 4-bytes length prefix
	// followed by the data).
	//
	// When encoding fixed-length byte array values, each value is expected to
	// be found back-to-back in chunks of the given size.
	EncodeLevels(dst, src []byte) ([]byte, error)
	EncodeBoolean(dst []byte, src []bool) ([]byte, error)
	EncodeInt32(dst []byte, src []int32) ([]byte, error)
	EncodeInt64(dst []byte, src []int64) ([]byte, error)
	EncodeInt96(dst []byte, src []deprecated.Int96) ([]byte, error)
	EncodeFloat(dst []byte, src []float32) ([]byte, error)
	EncodeDouble(dst []byte, src []float64) ([]byte, error)
	EncodeByteArray(dst, src []byte) ([]byte, error)
	EncodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error)

	// Decode methods deserialize from the source buffer into the destination
	// slice, potentially growing it if it was too short to contain the result.
	//
	// When decoding columns of byte array values, the values are written to the
	// output buffer using the PLAIN encoding.
	//
	// When encoding fixed-length byte array values, each value is written
	// back-to-back in chunks of the given size to the output buffer.
	DecodeLevels(dst, src []byte) ([]byte, error)
	DecodeBoolean(dst []bool, src []byte) ([]bool, error)
	DecodeInt32(dst []int32, src []byte) ([]int32, error)
	DecodeInt64(dst []int64, src []byte) ([]int64, error)
	DecodeInt96(dst []deprecated.Int96, src []byte) ([]deprecated.Int96, error)
	DecodeFloat(dst []float32, src []byte) ([]float32, error)
	DecodeDouble(dst []float64, src []byte) ([]float64, error)
	DecodeByteArray(dst, src []byte) ([]byte, error)
	DecodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error)
}
