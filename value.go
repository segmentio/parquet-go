package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"unsafe"
)

type Value struct {
	// data
	ptr *byte
	u64 uint64
	u32 uint32
	// type
	kind Kind // <<16 so the zero-value is <nil>
	// levels
	definitionLevel int32
	repetitionLevel int32
}

type ValueReader interface {
	ReadValue() (Value, error)
}

type ValueWriter interface {
	WriteValue(Value) error
}

const (
	valueKindShift = 16
)

func ValueOf(k Kind, v interface{}) Value {
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
		if vt := v.Type(); vt.Kind() == reflect.Array && vt.Elem().Kind() == reflect.Uint8 && vt.Len() == 12 {
			b := v.Slice(0, v.Len()).Bytes()
			return makeValueInt96(*(*[12]byte)(b))
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
				return makeValueByteArray(k, (*byte)(unsafe.Pointer(v.Pointer())), vt.Len())
			}
		}
	}

	panic("cannot create parquet value of type " + k.String() + " from go value of type " + v.Type().String())
}

func makeValueBoolean(value bool) Value {
	v := Value{kind: Boolean << valueKindShift}
	if value {
		v.u32 = 1
	}
	return v
}

func makeValueInt32(value int32) Value {
	return Value{
		kind: Int32 << valueKindShift,
		u32:  uint32(value),
	}
}

func makeValueInt64(value int64) Value {
	return Value{
		kind: Int64 << valueKindShift,
		u64:  uint64(value),
	}
}

func makeValueInt96(value [12]byte) Value {
	return Value{
		kind: Int96 << valueKindShift,
		u64:  binary.LittleEndian.Uint64(value[:8]),
		u32:  binary.LittleEndian.Uint32(value[8:]),
	}
}

func makeValueFloat(value float32) Value {
	return Value{
		kind: Float << valueKindShift,
		u32:  math.Float32bits(value),
	}
}

func makeValueDouble(value float64) Value {
	return Value{
		kind: Double << valueKindShift,
		u64:  math.Float64bits(value),
	}
}

func makeValueBytes(kind Kind, value []byte) Value {
	return makeValueByteArray(kind, *(**byte)(unsafe.Pointer(&value)), len(value))
}

func makeValueString(kind Kind, value string) Value {
	return makeValueByteArray(kind, *(**byte)(unsafe.Pointer(&value)), len(value))
}

func makeValueByteArray(kind Kind, data *byte, size int) Value {
	return Value{
		kind: kind << valueKindShift,
		ptr:  data,
		u64:  uint64(size),
	}
}

func (v Value) Kind() Kind { return v.kind >> valueKindShift }

func (v Value) IsNull() bool { return v.kind == 0 }

func (v Value) Boolean() bool { return v.u32 != 0 }

func (v Value) Int32() int32 { return int32(v.u32) }

func (v Value) Int64() int64 { return int64(v.u64) }

func (v Value) Int96() [12]byte { return makeInt96(v.u64, v.u32) }

func (v Value) Float() float32 { return math.Float32frombits(v.u32) }

func (v Value) Double() float64 { return math.Float64frombits(v.u64) }

func (v Value) ByteArray() []byte { return unsafe.Slice(v.ptr, int(v.u64)) }

func (v Value) DefinitionLevel() int32 { return v.definitionLevel }

func (v Value) RepetitionLevel() int32 { return v.repetitionLevel }

func (v *Value) SetDefinitionLevel(level int32) { v.definitionLevel = level }

func (v *Value) SetRepetitionLevel(level int32) { v.repetitionLevel = level }

func (v Value) Bytes() []byte { return v.AppendBytes(nil) }

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

func (v Value) String() string {
	switch v.Kind() {
	case Boolean:
		return strconv.FormatBool(v.Boolean())
	case Int32:
		return strconv.FormatInt(int64(v.Int32()), 10)
	case Int64:
		return strconv.FormatInt(v.Int64(), 10)
	case Int96:
		return fmt.Sprintf("%X", v.Int96())
	case Float:
		return strconv.FormatFloat(float64(v.Float()), 'g', -1, 32)
	case Double:
		return strconv.FormatFloat(v.Double(), 'g', -1, 64)
	case ByteArray, FixedLenByteArray:
		return strconv.Quote(string(v.ByteArray()))
	default:
		return "<nil>"
	}
}

func makeInt96(lo uint64, hi uint32) (i96 [12]byte) {
	binary.LittleEndian.PutUint64(i96[:8], uint64(lo))
	binary.LittleEndian.PutUint32(i96[8:], uint32(hi))
	return
}

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
	default:
		return false
	}
}
