package encoding

import (
	"fmt"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

type Kind int

const (
	Undefined Kind = iota
	Boolean
	Int32
	Int64
	Int96
	Float
	Double
	ByteArray
	FixedLenByteArray
	Level
)

func (kind Kind) String() string {
	switch kind {
	case Level:
		return "LEVEL"
	case Boolean:
		return "BOOLEAN"
	case Int32:
		return "INT32"
	case Int64:
		return "INT64"
	case Int96:
		return "INT96"
	case Float:
		return "FLOAT"
	case Double:
		return "DOUBLE"
	case ByteArray:
		return "BYTE_ARRAY"
	case FixedLenByteArray:
		return "FIXED_LEN_BYTE_ARRAY"
	default:
		return "UNDEFINED"
	}
}

type Values struct {
	kind    Kind
	size    int
	data    []byte
	offsets []uint32
}

func (values *Values) assertKind(kind Kind) {
	if kind != values.kind {
		panic(fmt.Sprintf("cannot convert values of type %s to type %s", values.kind, kind))
	}
}

func (values *Values) assertSize(size int) {
	if size != values.size {
		panic(fmt.Sprintf("cannot convert values of size %d to size %d", values.size, size))
	}
}

func (values *Values) Size() int64 {
	return int64(len(values.data))
}

func (values *Values) Kind() Kind {
	return values.kind
}

func (values *Values) Bytes(kind Kind) []byte {
	values.assertKind(kind)
	return values.data
}

func (values *Values) Level() []byte {
	values.assertKind(Level)
	return values.data
}

func (values *Values) Boolean() []byte {
	values.assertKind(Boolean)
	return values.data
}

func (values *Values) Int32() []int32 {
	values.assertKind(Int32)
	return unsafecast.BytesToInt32(values.data)
}

func (values *Values) Int64() []int64 {
	values.assertKind(Int64)
	return unsafecast.BytesToInt64(values.data)
}

func (values *Values) Int96() []deprecated.Int96 {
	values.assertKind(Int96)
	return deprecated.BytesToInt96(values.data)
}

func (values *Values) Float() []float32 {
	values.assertKind(Float)
	return unsafecast.BytesToFloat32(values.data)
}

func (values *Values) Double() []float64 {
	values.assertKind(Double)
	return unsafecast.BytesToFloat64(values.data)
}

func (values *Values) ByteArray() (data []byte, offsets []uint32) {
	values.assertKind(ByteArray)
	return values.data, values.offsets
}

func (values *Values) FixedLenByteArray() (data []byte, size int) {
	values.assertKind(FixedLenByteArray)
	return values.data, values.size
}

func (values *Values) Uint32() []uint32 {
	values.assertKind(Int32)
	return unsafecast.BytesToUint32(values.data)
}

func (values *Values) Uint64() []uint64 {
	values.assertKind(Int64)
	return unsafecast.BytesToUint64(values.data)
}

func (values *Values) Uint128() [][16]byte {
	values.assertKind(FixedLenByteArray)
	values.assertSize(16)
	return unsafecast.BytesToUint128(values.data)
}

func BooleanValues(values []byte) Values {
	return Values{
		kind: Boolean,
		data: values,
	}
}

func LevelValues(values []byte) Values {
	return Values{
		kind: Level,
		data: values,
	}
}

func Int32Values(values []int32) Values {
	return Values{
		kind: Int32,
		data: unsafecast.Int32ToBytes(values),
	}
}

func Int64Values(values []int64) Values {
	return Values{
		kind: Int64,
		data: unsafecast.Int64ToBytes(values),
	}
}

func Int96Values(values []deprecated.Int96) Values {
	return Values{
		kind: Int96,
		data: deprecated.Int96ToBytes(values),
	}
}

func FloatValues(values []float32) Values {
	return Values{
		kind: Float,
		data: unsafecast.Float32ToBytes(values),
	}
}

func DoubleValues(values []float64) Values {
	return Values{
		kind: Double,
		data: unsafecast.Float64ToBytes(values),
	}
}

func ByteArrayValues(values []byte, offsets []uint32) Values {
	return Values{
		kind:    ByteArray,
		data:    values,
		offsets: offsets,
	}
}

func FixedLenByteArrayValues(values []byte, size int) Values {
	return Values{
		kind: FixedLenByteArray,
		size: size,
		data: values,
	}
}

func Uint32Values(values []uint32) Values {
	return Int32Values(unsafecast.Uint32ToInt32(values))
}

func Uint64Values(values []uint64) Values {
	return Int64Values(unsafecast.Uint64ToInt64(values))
}

func Uint128Values(values [][16]byte) Values {
	return FixedLenByteArrayValues(unsafecast.Uint128ToBytes(values), 16)
}

func Int32ValuesFromBytes(values []byte) Values {
	return Values{
		kind: Int32,
		data: values,
	}
}

func Int64ValuesFromBytes(values []byte) Values {
	return Values{
		kind: Int64,
		data: values,
	}
}

func Int96ValuesFromBytes(values []byte) Values {
	return Values{
		kind: Int96,
		data: values,
	}
}

func FloatValuesFromBytes(values []byte) Values {
	return Values{
		kind: Float,
		data: values,
	}
}

func DoubleValuesFromBytes(values []byte) Values {
	return Values{
		kind: Double,
		data: values,
	}
}
