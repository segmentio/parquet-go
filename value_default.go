//go:build !go1.18

package parquet

import (
	"io"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/internal/bits"
)

// BooleanReader is an interface implemented by ValueReader instances which
// expose the content of a column of boolean values.
type BooleanReader interface {
	// Read boolean values into the buffer passed as argument.
	//
	// The method returns io.EOF when all values have been read.
	ReadBooleans(values []bool) (int, error)
}

// BooleanWriter is an interface implemented by ValueWriter instances which
// support writing columns of boolean values.
type BooleanWriter interface {
	// Write boolean values.
	//
	// The method returns the number of values written, and any error that
	// occured while writing the values.
	WriteBooleans(values []bool) (int, error)
}

// Int32Reader is an interface implemented by ValueReader instances which expose
// the content of a column of int32 values.
type Int32Reader interface {
	// Read 32 bits integer values into the buffer passed as argument.
	//
	// The method returns io.EOF when all values have been read.
	ReadInt32s(values []int32) (int, error)
}

// Int32Writer is an interface implemented by ValueWriter instances which
// support writing columns of 32 bits signed integer values.
type Int32Writer interface {
	// Write 32 bits signed integer values.
	//
	// The method returns the number of values written, and any error that
	// occured while writing the values.
	WriteInt32s(values []int32) (int, error)
}

// Int64Reader is an interface implemented by ValueReader instances which expose
// the content of a column of int64 values.
type Int64Reader interface {
	// Read 64 bits integer values into the buffer passed as argument.
	//
	// The method returns io.EOF when all values have been read.
	ReadInt64s(values []int64) (int, error)
}

// Int64Writer is an interface implemented by ValueWriter instances which
// support writing columns of 64 bits signed integer values.
type Int64Writer interface {
	// Write 64 bits signed integer values.
	//
	// The method returns the number of values written, and any error that
	// occured while writing the values.
	WriteInt64s(values []int64) (int, error)
}

// Int96Reader is an interface implemented by ValueReader instances which expose
// the content of a column of int96 values.
type Int96Reader interface {
	// Read 96 bits integer values into the buffer passed as argument.
	//
	// The method returns io.EOF when all values have been read.
	ReadInt96s(values []deprecated.Int96) (int, error)
}

// Int96Writer is an interface implemented by ValueWriter instances which
// support writing columns of 96 bits signed integer values.
type Int96Writer interface {
	// Write 96 bits signed integer values.
	//
	// The method returns the number of values written, and any error that
	// occured while writing the values.
	WriteInt96s(values []deprecated.Int96) (int, error)
}

// FloatReader is an interface implemented by ValueReader instances which expose
// the content of a column of single-precision floating point values.
type FloatReader interface {
	// Read single-precision floating point values into the buffer passed as
	// argument.
	//
	// The method returns io.EOF when all values have been read.
	ReadFloats(values []float32) (int, error)
}

// FloatWriter is an interface implemented by ValueWriter instances which
// support writing columns of single-precision floating point values.
type FloatWriter interface {
	// Write single-precision floating point values.
	//
	// The method returns the number of values written, and any error that
	// occured while writing the values.
	WriteFloats(values []float32) (int, error)
}

// DoubleReader is an interface implemented by ValueReader instances which
// expose the content of a column of double-precision float point values.
type DoubleReader interface {
	// Read double-precision floating point values into the buffer passed as
	// argument.
	//
	// The method returns io.EOF when all values have been read.
	ReadDoubles(values []float64) (int, error)
}

// DoubleWriter is an interface implemented by ValueWriter instances which
// support writing columns of double-precision floating point values.
type DoubleWriter interface {
	// Write double-precision floating point values.
	//
	// The method returns the number of values written, and any error that
	// occured while writing the values.
	WriteDoubles(values []float64) (int, error)
}

// ByteArrayReader is an interface implemented by ValueReader instances which
// expose the content of a column of variable length byte array values.
type ByteArrayReader interface {
	// Read values into the byte buffer passed as argument, returning the number
	// of values written to the buffer (not the number of bytes). Values are
	// written using the PLAIN encoding, each byte array prefixed with its
	// length encoded as a 4 bytes little endian unsigned integer.
	//
	// The method returns io.EOF when all values have been read.
	//
	// If the buffer was not empty, but too small to hold at least one value,
	// io.ErrShortBuffer is returned.
	ReadByteArrays(values []byte) (int, error)
}

// ByteArrayWriter is an interface implemented by ValueWriter instances which
// support writing columns of variable length byte array values.
type ByteArrayWriter interface {
	// Write variable length byte array values.
	//
	// The values passed as input must be laid out using the PLAIN encoding,
	// with each byte array prefixed with the four bytes little endian unsigned
	// integer length.
	//
	// The method returns the number of values written to the underlying column
	// (not the number of bytes), or any error that occured while attempting to
	// write the values.
	WriteByteArrays(values []byte) (int, error)
}

// FixedLenByteArrayReader is an interface implemented by ValueReader instances
// which expose the content of a column of fixed length byte array values.
type FixedLenByteArrayReader interface {
	// Read values into the byte buffer passed as argument, returning the number
	// of values written to the buffer (not the number of bytes).
	//
	// The method returns io.EOF when all values have been read.
	//
	// If the buffer was not empty, but too small to hold at least one value,
	// io.ErrShortBuffer is returned.
	ReadFixedLenByteArrays(values []byte) (int, error)
}

// FixedLenByteArrayWriter is an interface implemented by ValueWriter instances
// which support writing columns of fixed length byte array values.
type FixedLenByteArrayWriter interface {
	// Writes the fixed length byte array values.
	//
	// The size of the values is assumed to be the same as the expected size of
	// items in the column. The method errors if the length of the input values
	// is not a multiple of the expected item size.
	WriteFixedLenByteArrays(values []byte) (int, error)
}

type booleanValueReader struct {
	values      []bool
	offset      int
	columnIndex int16
}

func (r *booleanValueReader) Read(b []byte) (n int, err error) {
	return r.ReadBooleans(bits.BytesToBool(b))
}

func (r *booleanValueReader) ReadBooleans(values []bool) (n int, err error) {
	n = copy(values, r.values[r.offset:])
	r.offset += n
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

func (r *booleanValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.values) {
		values[n] = makeValueBoolean(r.values[r.offset])
		values[n].columnIndex = r.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

type int32ValueReader struct {
	values      []int32
	offset      int
	columnIndex int16
}

func (r *int32ValueReader) Read(b []byte) (n int, err error) {
	n, err = r.ReadInt32s(bits.BytesToInt32(b))
	return 4 * n, err
}

func (r *int32ValueReader) ReadInt32s(values []int32) (n int, err error) {
	n = copy(values, r.values[r.offset:])
	r.offset += n
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

func (r *int32ValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.values) {
		values[n] = makeValueInt32(r.values[r.offset])
		values[n].columnIndex = r.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

type int64ValueReader struct {
	values      []int64
	offset      int
	columnIndex int16
}

func (r *int64ValueReader) Read(b []byte) (n int, err error) {
	n, err = r.ReadInt64s(bits.BytesToInt64(b))
	return 8 * n, err
}

func (r *int64ValueReader) ReadInt64s(values []int64) (n int, err error) {
	n = copy(values, r.values[r.offset:])
	r.offset += n
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

func (r *int64ValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.values) {
		values[n] = makeValueInt64(r.values[r.offset])
		values[n].columnIndex = r.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

type int96ValueReader struct {
	values      []deprecated.Int96
	offset      int
	columnIndex int16
}

func (r *int96ValueReader) Read(b []byte) (n int, err error) {
	n, err = r.ReadInt96s(bits.BytesToInt32(b))
	return 12 * n, err
}

func (r *int96ValueReader) ReadInt96s(values []deprecated.Int96) (n int, err error) {
	n = copy(values, r.values[r.offset:])
	r.offset += n
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

func (r *int96ValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.values) {
		values[n] = makeValueInt96(r.values[r.offset])
		values[n].columnIndex = r.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

type floatValueReader struct {
	values      []float32
	offset      int
	columnIndex int16
}

func (r *floatValueReader) Read(b []byte) (n int, err error) {
	n, err = r.ReadFloats(bits.BytesToInt32(b))
	return 4 * n, err
}

func (r *floatValueReader) ReadFloats(values []float32) (n int, err error) {
	n = copy(values, r.values[r.offset:])
	r.offset += n
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

func (r *floatValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.values) {
		values[n] = makeValueFloat(r.values[r.offset])
		values[n].columnIndex = r.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

type doubleValueReader struct {
	values      []float64
	offset      int
	columnIndex double
}

func (r *doubleValueReader) Read(b []byte) (n int, err error) {
	n, err = r.ReadDoubles(bits.BytesToInt32(b))
	return 8 * n, err
}

func (r *doubleValueReader) ReadDoubles(values []float64) (n int, err error) {
	n = copy(values, r.values[r.offset:])
	r.offset += n
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

func (r *doubleValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.offset < len(r.values) {
		values[n] = makeValueDouble(r.values[r.offset])
		values[n].columnIndex = r.columnIndex
		r.offset++
		n++
	}
	if r.offset == len(r.values) {
		err = io.EOF
	}
	return n, err
}

var (
	_ io.Reader = (*booleanValueReader)(nil)
	_ io.Reader = (*int32ValueReader)(nil)
	_ io.Reader = (*int64ValueReader)(nil)
	_ io.Reader = (*int96ValueReader)(nil)
	_ io.Reader = (*floatValueReader)(nil)
	_ io.Reader = (*doubleValueReader)(nil)
	_ io.Reader = (*byteArrayValueReader)(nil)
	_ io.Reader = (*fixedLenByteArrayValueReader)(nil)

	_ BooleanReader           = (*booleanValueReader)(nil)
	_ Int32Reader             = (*int32ValueReader)(nil)
	_ Int64Reader             = (*int64ValueReader)(nil)
	_ Int96Reader             = (*int96ValueReader)(nil)
	_ FloatReader             = (*floatValueReader)(nil)
	_ DoubleReader            = (*doubleValueReader)(nil)
	_ ByteArrayReader         = (*byteArrayValueReader)(nil)
	_ FixedLenByteArrayReader = (*fixedLenByteArrayValueReader)(nil)
)
