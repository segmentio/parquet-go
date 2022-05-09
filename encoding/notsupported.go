package encoding

import (
	"errors"
	"fmt"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/format"
)

var (
	// ErrNotSupported is an error returned when the underlying encoding does
	// not support the type of values being encoded or decoded.
	//
	// This error may be wrapped with type information, applications must use
	// errors.Is rather than equality comparisons to test the error values
	// returned by encoders and decoders.
	ErrNotSupported = errors.New("encoding not supported")

	// ErrInvalidArgument is an error returned one or more arguments passed to
	// the encoding functions are incorrect.
	//
	// As with ErrNotSupported, this error may be wrapped with specific
	// information about the problem and applications are expected to use
	// errors.Is for comparisons.
	ErrInvalidArgument = errors.New("invalid argument")
)

// Error constructs an error which wraps err and indicates that it originated
// from the given encoding.
func Error(e Encoding, err error) error {
	return fmt.Errorf("%s: %w", e, err)
}

// Errorf is like Error but constructs the error message from the given format
// and arguments.
func Errorf(e Encoding, msg string, args ...interface{}) error {
	return Error(e, fmt.Errorf(msg, args...))
}

// ErrInvalidInputSize constructs an error indicating that decoding failed due
// to the size of the input.
func ErrInvalidInputSize(e Encoding, typ string, size int) error {
	return Errorf(e, "cannot decode %s from input of size %d: %w", typ, size, ErrInvalidArgument)
}

// CanEncodeBoolean returns true if the e can encode BOOLEAN values.
func CanEncodeBoolean(e Encoding) bool {
	_, err := e.EncodeBoolean(nil, nil)
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeInt8 returns true if the e can encode INT8 values.
func CanEncodeInt8(e Encoding) bool {
	_, err := e.EncodeInt8(nil, nil)
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeInt32 returns true if the e can encode INT32 values.
func CanEncodeInt32(e Encoding) bool {
	_, err := e.EncodeInt32(nil, nil)
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeInt64 returns true if the e can encode INT64 values.
func CanEncodeInt64(e Encoding) bool {
	_, err := e.EncodeInt64(nil, nil)
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeInt96 returns true if the e can encode INT96 values.
func CanEncodeInt96(e Encoding) bool {
	_, err := e.EncodeInt96(nil, nil)
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeFloat returns true if the e can encode FLOAT values.
func CanEncodeFloat(e Encoding) bool {
	_, err := e.EncodeFloat(nil, nil)
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeDouble returns true if the e can encode DOUBLE values.
func CanEncodeDouble(e Encoding) bool {
	_, err := e.EncodeDouble(nil, nil)
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeByteArray returns true if the e can encode BYTE_ARRAY values.
func CanEncodeByteArray(e Encoding) bool {
	_, err := e.EncodeByteArray(nil, nil)
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeFixedLenByteArray returns true if the e can encode
// FIXED_LEN_BYTE_ARRAY values.
func CanEncodeFixedLenByteArray(e Encoding) bool {
	_, err := e.EncodeFixedLenByteArray(nil, nil, 1)
	return !errors.Is(err, ErrNotSupported)
}

// NotSupported is a type satisfying the Encoding interface which does not
// support encoding nor decoding any value types.
type NotSupported struct {
}

func (NotSupported) String() string {
	return "NOT_SUPPORTED"
}

func (NotSupported) Encoding() format.Encoding {
	return -1
}

func (NotSupported) EncodeBoolean(dst []byte, src []bool) ([]byte, error) {
	return dst[:0], errNotSupported("BOOLEAN")
}

func (NotSupported) EncodeInt8(dst []byte, src []int8) ([]byte, error) {
	return dst[:0], errNotSupported("INT8")
}

func (NotSupported) EncodeInt32(dst []byte, src []int32) ([]byte, error) {
	return dst[:0], errNotSupported("INT32")
}

func (NotSupported) EncodeInt64(dst []byte, src []int64) ([]byte, error) {
	return dst[:0], errNotSupported("INT64")
}

func (NotSupported) EncodeInt96(dst []byte, src []deprecated.Int96) ([]byte, error) {
	return dst[:0], errNotSupported("INT96")
}

func (NotSupported) EncodeFloat(dst []byte, src []float32) ([]byte, error) {
	return dst[:0], errNotSupported("FLOAT")
}

func (NotSupported) EncodeDouble(dst []byte, src []float64) ([]byte, error) {
	return dst[:0], errNotSupported("DOUBLE")
}

func (NotSupported) EncodeByteArray(dst, src []byte) ([]byte, error) {
	return dst[:0], errNotSupported("BYTE_ARRAY")
}

func (NotSupported) EncodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error) {
	return dst[:0], errNotSupported("FIXED_LEN_BYTE_ARRAY")
}

func (NotSupported) DecodeBoolean(dst []bool, src []byte) ([]bool, error) {
	return dst[:0], errNotSupported("BOOLEAN")
}

func (NotSupported) DecodeInt8(dst []int8, src []byte) ([]int8, error) {
	return dst[:0], errNotSupported("INT8")
}

func (NotSupported) DecodeInt32(dst []int32, src []byte) ([]int32, error) {
	return dst[:0], errNotSupported("INT32")
}

func (NotSupported) DecodeInt64(dst []int64, src []byte) ([]int64, error) {
	return dst[:0], errNotSupported("INT64")
}

func (NotSupported) DecodeInt96(dst []deprecated.Int96, src []byte) ([]deprecated.Int96, error) {
	return dst[:0], errNotSupported("INT96")
}

func (NotSupported) DecodeFloat(dst []float32, src []byte) ([]float32, error) {
	return dst[:0], errNotSupported("FLOAT")
}

func (NotSupported) DecodeDouble(dst []float64, src []byte) ([]float64, error) {
	return dst[:0], errNotSupported("DOUBLE")
}

func (NotSupported) DecodeByteArray(dst, src []byte) ([]byte, error) {
	return dst[:0], errNotSupported("BYTE_ARRAY")
}

func (NotSupported) DecodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error) {
	return dst[:0], errNotSupported("FIXED_LEN_BYTE_ARRAY")
}

func errNotSupported(typ string) error {
	return fmt.Errorf("%w for type %s", ErrNotSupported, typ)
}

var (
	_ Encoding = NotSupported{}
)
