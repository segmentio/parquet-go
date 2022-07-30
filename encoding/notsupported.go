package encoding

import (
	"errors"
	"fmt"

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

// ErrEncodeInvalidInputSize constructs an error indicating that encoding failed
// due to the size of the input.
func ErrEncodeInvalidInputSize(e Encoding, typ string, size int) error {
	return errInvalidInputSize(e, "encode", typ, size)
}

// ErrDecodeInvalidInputSize constructs an error indicating that decoding failed
// due to the size of the input.
func ErrDecodeInvalidInputSize(e Encoding, typ string, size int) error {
	return errInvalidInputSize(e, "decode", typ, size)
}

func errInvalidInputSize(e Encoding, op, typ string, size int) error {
	return Errorf(e, "cannot %s %s from input of size %d: %w", op, typ, size, ErrInvalidArgument)
}

// CanEncodeInt8 reports whether e can encode LEVELS values.
func CanEncodeLevels(e Encoding) bool {
	_, err := e.EncodeLevels(nil, LevelValues(nil))
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeBoolean reports whether e can encode BOOLEAN values.
func CanEncodeBoolean(e Encoding) bool {
	_, err := e.EncodeBoolean(nil, BooleanValues(nil))
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeInt32 reports whether e can encode INT32 values.
func CanEncodeInt32(e Encoding) bool {
	_, err := e.EncodeInt32(nil, Int32Values(nil))
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeInt64 reports whether e can encode INT64 values.
func CanEncodeInt64(e Encoding) bool {
	_, err := e.EncodeInt64(nil, Int64Values(nil))
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeInt96 reports whether e can encode INT96 values.
func CanEncodeInt96(e Encoding) bool {
	_, err := e.EncodeInt96(nil, Int96Values(nil))
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeFloat reports whether e can encode FLOAT values.
func CanEncodeFloat(e Encoding) bool {
	_, err := e.EncodeFloat(nil, FloatValues(nil))
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeDouble reports whether e can encode DOUBLE values.
func CanEncodeDouble(e Encoding) bool {
	_, err := e.EncodeDouble(nil, DoubleValues(nil))
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeByteArray reports whether e can encode BYTE_ARRAY values.
func CanEncodeByteArray(e Encoding) bool {
	_, err := e.EncodeByteArray(nil, ByteArrayValues(nil, nil))
	return !errors.Is(err, ErrNotSupported)
}

// CanEncodeFixedLenByteArray reports whether e can encode
// FIXED_LEN_BYTE_ARRAY values.
func CanEncodeFixedLenByteArray(e Encoding) bool {
	_, err := e.EncodeFixedLenByteArray(nil, FixedLenByteArrayValues(nil, 1))
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

func (NotSupported) EncodeLevels(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("LEVELS")
}

func (NotSupported) EncodeBoolean(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("BOOLEAN")
}

func (NotSupported) EncodeInt32(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("INT32")
}

func (NotSupported) EncodeInt64(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("INT64")
}

func (NotSupported) EncodeInt96(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("INT96")
}

func (NotSupported) EncodeFloat(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("FLOAT")
}

func (NotSupported) EncodeDouble(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("DOUBLE")
}

func (NotSupported) EncodeByteArray(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("BYTE_ARRAY")
}

func (NotSupported) EncodeFixedLenByteArray(dst []byte, src Values) ([]byte, error) {
	return dst[:0], errNotSupported("FIXED_LEN_BYTE_ARRAY")
}

func (NotSupported) DecodeLevels(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("LEVELS")
}

func (NotSupported) DecodeBoolean(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("BOOLEAN")
}

func (NotSupported) DecodeInt32(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("INT32")
}

func (NotSupported) DecodeInt64(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("INT64")
}

func (NotSupported) DecodeInt96(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("INT96")
}

func (NotSupported) DecodeFloat(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("FLOAT")
}

func (NotSupported) DecodeDouble(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("DOUBLE")
}

func (NotSupported) DecodeByteArray(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("BYTE_ARRAY")
}

func (NotSupported) DecodeFixedLenByteArray(dst Values, src []byte) (Values, error) {
	return dst, errNotSupported("FIXED_LEN_BYTE_ARRAY")
}

func errNotSupported(typ string) error {
	return fmt.Errorf("%w for type %s", ErrNotSupported, typ)
}

var (
	_ Encoding = NotSupported{}
)
