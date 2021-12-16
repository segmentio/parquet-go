package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"unsafe"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/deprecated"
)

// The Value type is similar to the reflect.Value abstraction of Go values, but
// for parquet values. Value instances wrap underlying Go values mapped to one
// of the parquet physical types.
//
// Value instances are small, immutable objects, and usually passed by value
// between function calls.
//
// The zero-value of Value represents the null parquet value.
type Value struct {
	// data
	ptr *byte
	u64 uint64
	u32 uint32
	// type
	kind int16 // XOR(Kind) so the zero-value is <null>
	// levels
	definitionLevel int8
	repetitionLevel int8
}

// The ValueReader interface is implemented by types that read sequences of
// parquet values.
type ValueReader interface {
	// Reads the next value from the sequence, or returns io.EOF when the end
	// of the sequence was reached, or another error if no values could be read.
	ReadValue() (Value, error)
}

// The ValueWriter interface is implemented by types that write sequences of
// parquet values.
type ValueWriter interface {
	// Writes the next value to the sequence, returning a non-nil error if the
	// write failed.
	WriteValue(Value) error
}

// ValueOf constructs a parquet value from a Go value v.
//
// The physical type of the value is assumed from the Go type of v using the
// following conversion table:
//
//	Go type | Parquet physical type
//	------- | ---------------------
//	nil     | NULL
//	bool    | BOOLEAN
//	int8    | INT32
//	int16   | INT32
//	int32   | INT32
//	int64   | INT64
//	int     | INT64
//	uint8   | INT32
//	uint16  | INT32
//	uint32  | INT32
//	uint64  | INT64
//	uintptr | INT64
//	float32 | FLOAT
//	float64 | DOUBLE
//	string  | BYTE_ARRAY
//	[]byte  | BYTE_ARRAY
//	[*]byte | FIXED_LEN_BYTE_ARRAY
//
// When converting a []byte or [*]byte value, the underlying byte array is not
// copied; instead, the returned parquet value holds a reference to it.
//
// The repetition and definition levels of the returned value are both zero.
//
// The function panics if the Go value cannot be represented in parquet.
func ValueOf(v interface{}) Value {
	switch value := v.(type) {
	case nil:
		return Value{}
	case uuid.UUID:
		return makeValueBytes(FixedLenByteArray, value[:])
	}

	k := Kind(-1)
	t := reflect.TypeOf(v)

	switch t.Kind() {
	case reflect.Bool:
		k = Boolean
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		k = Int32
	case reflect.Int64, reflect.Int, reflect.Uint64, reflect.Uint, reflect.Uintptr:
		k = Int64
	case reflect.Float32:
		k = Float
	case reflect.Float64:
		k = Double
	case reflect.String:
		k = ByteArray
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			k = ByteArray
		}
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			k = FixedLenByteArray
		}
	}

	if k < 0 {
		panic("cannot create parquet value from go value of type " + t.String())
	}

	return makeValue(k, reflect.ValueOf(v))
}

func makeValue(k Kind, v reflect.Value) Value {
	if !v.IsValid() {
		return Value{}
	}

	switch k {
	case Boolean:
		return makeValueBoolean(v.Bool())

	case Int32:
		switch v.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32:
			return makeValueInt32(int32(v.Int()))
		case reflect.Uint8, reflect.Uint16, reflect.Uint32:
			return makeValueInt32(int32(v.Uint()))
		}

	case Int64:
		switch v.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return makeValueInt64(v.Int())
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return makeValueInt64(int64(v.Uint()))
		}

	case Int96:
		switch v.Type() {
		case reflect.TypeOf(deprecated.Int96{}):
			return makeValueInt96(v.Interface().(deprecated.Int96))
		}

	case Float:
		switch v.Kind() {
		case reflect.Float32:
			return makeValueFloat(float32(v.Float()))
		}

	case Double:
		switch v.Kind() {
		case reflect.Float32, reflect.Float64:
			return makeValueDouble(v.Float())
		}

	case ByteArray:
		switch vt := v.Type(); vt.Kind() {
		case reflect.String:
			return makeValueString(k, v.String())
		case reflect.Slice:
			if vt.Elem().Kind() == reflect.Uint8 {
				return makeValueBytes(k, v.Bytes())
			}
		}

	case FixedLenByteArray:
		switch vt := v.Type(); vt.Kind() {
		case reflect.String: // uuid
			return makeValueString(k, v.String())
		case reflect.Array:
			if vt.Elem().Kind() == reflect.Uint8 {
				return makeValueFixedLenByteArray(v)
			}
		}
	}

	panic("cannot create parquet value of type " + k.String() + " from go value of type " + v.Type().String())
}

func makeValueBoolean(value bool) Value {
	v := Value{kind: ^int16(Boolean)}
	if value {
		v.u32 = 1
	}
	return v
}

func makeValueInt32(value int32) Value {
	return Value{
		kind: ^int16(Int32),
		u32:  uint32(value),
	}
}

func makeValueInt64(value int64) Value {
	return Value{
		kind: ^int16(Int64),
		u64:  uint64(value),
	}
}

func makeValueInt96(value deprecated.Int96) Value {
	return Value{
		kind: ^int16(Int96),
		u64:  uint64(value[0]) | (uint64(value[1]) << 32),
		u32:  value[2],
	}
}

func makeValueFloat(value float32) Value {
	return Value{
		kind: ^int16(Float),
		u32:  math.Float32bits(value),
	}
}

func makeValueDouble(value float64) Value {
	return Value{
		kind: ^int16(Double),
		u64:  math.Float64bits(value),
	}
}

func makeValueBytes(kind Kind, value []byte) Value {
	return makeValueByteArray(kind, *(**byte)(unsafe.Pointer(&value)), len(value))
}

func makeValueString(kind Kind, value string) Value {
	return makeValueByteArray(kind, *(**byte)(unsafe.Pointer(&value)), len(value))
}

func makeValueFixedLenByteArray(v reflect.Value) Value {
	t := v.Type()
	// When the array is addressable, we take advantage of this
	// condition to avoid the heap allocation otherwise needed
	// to pack the reference into an interface{} value.
	if v.CanAddr() {
		v = v.Addr()
	} else {
		u := reflect.New(t)
		u.Elem().Set(v)
		v = u
	}
	return makeValueByteArray(FixedLenByteArray, (*byte)(unsafe.Pointer(v.Pointer())), t.Len())
}

func makeValueByteArray(kind Kind, data *byte, size int) Value {
	return Value{
		kind: ^int16(kind),
		ptr:  data,
		u64:  uint64(size),
	}
}

// Kind returns the kind of v, which represents its parquet physical type.
func (v Value) Kind() Kind { return ^Kind(v.kind) }

// IsNull returns true if v is the null value.
func (v Value) IsNull() bool { return v.kind == 0 }

// Boolean returns v as a bool, assuming the underlying type is BOOLEAN.
func (v Value) Boolean() bool { return v.u32 != 0 }

// Int32 returns v as a int32, assuming the underlying type is INT32.
func (v Value) Int32() int32 { return int32(v.u32) }

// Int64 returns v as a int64, assuming the underlying type is INT64.
func (v Value) Int64() int64 { return int64(v.u64) }

// Int96 returns v as a int96, assuming the underlying type is INT96.
func (v Value) Int96() deprecated.Int96 { return makeInt96(v.u64, v.u32) }

// Float returns v as a float32, assuming the underlying type is FLOAT.
func (v Value) Float() float32 { return math.Float32frombits(v.u32) }

// Double returns v as a float64, assuming the underlying type is DOUBLE.
func (v Value) Double() float64 { return math.Float64frombits(v.u64) }

// ByteArray returns v as a []byte, assuming the underlying type is either
// BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY.
func (v Value) ByteArray() []byte { return unsafe.Slice(v.ptr, int(v.u64)) }

// RepetitionLevel returns the repetition level of v.
func (v Value) RepetitionLevel() int8 { return v.repetitionLevel }

// DefinitionLevel returns the definition level of v.
func (v Value) DefinitionLevel() int8 { return v.definitionLevel }

// Bytes returns the binary representation of v.
//
// If v is the null value, an nil byte slice is returned.
func (v Value) Bytes() []byte { return v.AppendBytes(nil) }

// AppendBytes appends the binary representation of v to b.
//
// If v is the null value, b is returned unchanged.
func (v Value) AppendBytes(b []byte) []byte {
	buf := [12]byte{}
	switch v.Kind() {
	case Boolean:
		binary.LittleEndian.PutUint32(buf[:4], v.u32)
		return append(b, buf[0])
	case Int32, Float:
		binary.LittleEndian.PutUint32(buf[:4], v.u32)
		return append(b, buf[:4]...)
	case Int64, Double:
		binary.LittleEndian.PutUint64(buf[:8], v.u64)
		return append(b, buf[:8]...)
	case Int96:
		binary.LittleEndian.PutUint64(buf[:8], v.u64)
		binary.LittleEndian.PutUint32(buf[8:], v.u32)
		return append(b, buf[:12]...)
	case ByteArray, FixedLenByteArray:
		return append(b, v.ByteArray()...)
	default:
		return b
	}
}

// Format outputs a human-readable representation of v to w, using r as the
// formatting verb to describe how the value should be printed.
//
// The following formatting options are supported:
//
//	%d	prints the definition level
//	%+d	prints the definition level, prefixed with "D:"
//	%r	prints the repetition level
//	%+r	prints the repetition level, prefixed with "R:"
//	%q	prints the quoted representation of v
//	%+q	prints the quoted representation of v, prefixed with "V:"
//	%s	prints the string representation of v
//	%+s	prints the string representation of v, prefixed with "V:"
//	%v	same as %s
//	%+v	same as %+s
//	%#v	prints a Go value representation of v
//
// Format satisfies the fmt.Formatter interface.
func (v Value) Format(w fmt.State, r rune) {
	switch r {
	case 'd':
		if w.Flag('+') {
			io.WriteString(w, "D:")
		}
		fmt.Fprint(w, v.definitionLevel)

	case 'r':
		if w.Flag('+') {
			io.WriteString(w, "R:")
		}
		fmt.Fprint(w, v.repetitionLevel)

	case 'q':
		if w.Flag('+') {
			io.WriteString(w, "V:")
		}
		switch v.Kind() {
		case ByteArray, FixedLenByteArray:
			fmt.Fprintf(w, "%q", v.ByteArray())
		default:
			fmt.Fprintf(w, `"%s"`, v)
		}

	case 's':
		if w.Flag('+') {
			io.WriteString(w, "V:")
		}
		switch v.Kind() {
		case Boolean:
			fmt.Fprint(w, v.Boolean())
		case Int32:
			fmt.Fprint(w, v.Int32())
		case Int64:
			fmt.Fprint(w, v.Int64())
		case Int96:
			fmt.Fprint(w, v.Int96())
		case Float:
			fmt.Fprint(w, v.Float())
		case Double:
			fmt.Fprint(w, v.Double())
		case ByteArray, FixedLenByteArray:
			w.Write(v.ByteArray())
		default:
			io.WriteString(w, "<null>")
		}

	case 'v':
		switch {
		case w.Flag('+'):
			fmt.Fprintf(w, "%+[1]d %+[1]r %+[1]s", v)
		case w.Flag('#'):
			fmt.Fprintf(w, "parquet.Value{%+[1]d,%+[1]d,%+[1]s}", v)
		default:
			v.Format(w, 's')
		}
	}
}

// String returns a string representation of v.
func (v Value) String() string {
	return fmt.Sprintf("%s", v)
}

// GoString returns a Go value string representation of v.
func (v Value) GoString() string {
	return fmt.Sprintf("%#v", v)
}

// Level returns v with the repetition and definition levels set to the values
// passed as arguments.
//
// The method panics if either argument is negative.
func (v Value) Level(repetitionLevel, definitionLevel int8) Value {
	if repetitionLevel < 0 {
		panic("cannot create a value with a negative repetition level")
	}
	if definitionLevel < 0 {
		panic("cannot create a value with a negative definition level")
	}
	v.repetitionLevel = repetitionLevel
	v.definitionLevel = definitionLevel
	return v
}

func makeInt96(lo uint64, hi uint32) (i96 deprecated.Int96) {
	return deprecated.Int96{
		0: uint32(lo),
		1: uint32(lo >> 32),
		2: hi,
	}
}

// Equal returns true if v1 and v2 are equal.
//
// Values are considered equal if they are of the same physical typee and hold
// the same Go values. For BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY, the content of
// the underlying byte arrays are tested for equality.
func Equal(v1, v2 Value) bool {
	if v1.kind != v2.kind {
		return false
	}
	switch v1.Kind() {
	case Boolean:
		return v1.Boolean() == v2.Boolean()
	case Int32:
		return v1.Int32() == v2.Int32()
	case Int64:
		return v1.Int64() == v2.Int64()
	case Int96:
		return v1.Int96() == v2.Int96()
	case Float:
		return v1.Float() == v2.Float()
	case Double:
		return v1.Double() == v2.Double()
	case ByteArray, FixedLenByteArray:
		return bytes.Equal(v1.ByteArray(), v2.ByteArray())
	case -1: // null
		return true
	default:
		return false
	}
}

var (
	_ fmt.Formatter = Value{}
	_ fmt.Stringer  = Value{}
)

/*
type ValueIter struct {
	reader ValueReader
	value  Value
	err    error
}

func NewValueIter(r ValueReader) *ValueIter {
	return &ValueIter{reader: r}
}

func (it *ValueIter) Reset(r ValueReader) {
	it.reader, it.value, it.err = r, Value{}, nil
}

func (it *ValueIter) Next() bool {
	if it.reader == nil {
		return false
	}

	v, err := it.reader.ReadValue()
	if err != nil {
		if it.err != io.EOF {
			it.err = err
		}
		it.reader = nil
		it.value = Value{}
		return false
	}

	it.value = v
	return true
}

func (it *ValueIter) Done() bool {
	return it.reader == nil
}

func (it *ValueIter) Err() error {
	return it.err
}

func (it *ValueIter) Value() Value {
	return it.value
}
*/
