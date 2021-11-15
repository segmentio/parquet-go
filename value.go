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
	kind Kind // +1 so the zero-value is <nil>
	// levels
	definitionLevel int32
	repetitionLevel int32
}

func ValueOf(k Kind, v interface{}) Value {
	if v == nil {
		return Value{kind: k + 1}
	}
	return makeValue(k, reflect.ValueOf(v))
}

func makeValue(k Kind, v reflect.Value) Value {
	switch k {
	case Boolean:
		return makeValueBoolean(k, v.Bool())

	case Int32:
		switch v.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32:
			return makeValueInt32(k, int32(v.Int()))

		case reflect.Uint8, reflect.Uint16, reflect.Uint32:
			return makeValueInt32(k, int32(v.Uint()))
		}

	case Int64:
		switch v.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			return makeValueInt64(k, v.Int())

		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
			return makeValueInt64(k, int64(v.Uint()))
		}

	case Int96:
		if vt := v.Type(); vt.Kind() == reflect.Array && vt.Elem().Kind() == reflect.Uint8 && vt.Len() == 12 {
			b := v.Slice(0, v.Len()).Bytes()
			return makeValueInt96(k, b)
		}

	case Float:
		switch v.Kind() {
		case reflect.Float32:
			return makeValueFloat(k, float32(v.Float()))
		}

	case Double:
		switch v.Kind() {
		case reflect.Float32, reflect.Float64:
			return makeValueDouble(k, v.Float())
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

func makeValueBoolean(kind Kind, value bool) Value {
	v := Value{kind: kind + 1}
	if value {
		v.u32 = 1
	}
	return v
}

func makeValueInt32(kind Kind, value int32) Value {
	return Value{
		kind: kind + 1,
		u32:  uint32(value),
	}
}

func makeValueInt64(kind Kind, value int64) Value {
	return Value{
		kind: kind + 1,
		u64:  uint64(value),
	}
}

func makeValueInt96(kind Kind, value []byte) Value {
	return Value{
		kind: kind + 1,
		u64:  binary.LittleEndian.Uint64(value[:8]),
		u32:  binary.LittleEndian.Uint32(value[8:]),
	}
}

func makeValueFloat(kind Kind, value float32) Value {
	return Value{
		kind: kind + 1,
		u32:  math.Float32bits(value),
	}
}

func makeValueDouble(kind Kind, value float64) Value {
	return Value{
		kind: kind + 1,
		u64:  math.Float64bits(value),
	}
}

func makeValueBytes(kind Kind, value []byte) Value {
	return Value{
		kind: kind + 1,
		ptr:  *(**byte)(unsafe.Pointer(&value)),
		u64:  uint64(len(value)),
	}
}

func makeValueString(kind Kind, value string) Value {
	return Value{
		kind: kind + 1,
		ptr:  *(**byte)(unsafe.Pointer(&value)),
		u64:  uint64(len(value)),
	}
}

func makeValueByteArray(kind Kind, data *byte, size int) Value {
	return Value{
		kind: kind + 1,
		ptr:  data,
		u64:  uint64(size),
	}
}

func (v Value) Kind() Kind { return v.kind - 1 }

func (v Value) IsNull() bool { return v.kind == 0 }

func (v Value) Boolean() bool { return v.u32 != 0 }

func (v Value) Int32() int32 { return int32(v.u32) }

func (v Value) Int64() int64 { return int64(v.u64) }

func (v Value) Int96() [12]byte { return makeInt96(v.u64, v.u32) }

func (v Value) Float() float32 { return math.Float32frombits(v.u32) }

func (v Value) Double() float64 { return math.Float64frombits(v.u64) }

func (v Value) ByteArray() []byte { return unsafe.Slice(v.ptr, int(v.u64)) }

func (v Value) DefinitionLevel() int { return int(v.definitionLevel) }

func (v Value) RepetitionLevel() int { return int(v.repetitionLevel) }

func (v *Value) SetDefinitionLevel(level int) { v.definitionLevel = int32(level) }

func (v *Value) SetRepetitionLevel(level int) { v.repetitionLevel = int32(level) }

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
