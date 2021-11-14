package parquet

import (
	"fmt"
	"time"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/format"
)

type Kind int32

const (
	Boolean           Kind = Kind(format.Boolean)
	Int32             Kind = Kind(format.Int32)
	Int64             Kind = Kind(format.Int64)
	Int96             Kind = Kind(format.Int96)
	Float             Kind = Kind(format.Float)
	Double            Kind = Kind(format.Double)
	ByteArray         Kind = Kind(format.ByteArray)
	FixedLenByteArray Kind = Kind(format.FixedLenByteArray)
)

func (k Kind) String() string {
	return format.Type(k).String()
}

type Type interface {
	Kind() Kind

	Length() int

	LogicalType() format.LogicalType

	ConvertedType() deprecated.ConvertedType

	NewPageBuffer(bufferSize int) PageBuffer
}

type primitiveType struct{}

func (t primitiveType) LogicalType() format.LogicalType { return format.LogicalType{} }

func (t primitiveType) ConvertedType() deprecated.ConvertedType { return -1 }

type booleanType struct{ primitiveType }

func (t booleanType) Kind() Kind { return Boolean }

func (t booleanType) Length() int { return 1 }

func (t booleanType) NewPageBuffer(bufferSize int) PageBuffer {
	return newBooleanPageBuffer(t, bufferSize)
}

type int32Type struct{ primitiveType }

func (t int32Type) Kind() Kind { return Int32 }

func (t int32Type) Length() int { return 32 }

func (t int32Type) NewPageBuffer(bufferSize int) PageBuffer { return newInt32PageBuffer(t, bufferSize) }

type int64Type struct{ primitiveType }

func (t int64Type) Kind() Kind { return Int64 }

func (t int64Type) Length() int { return 64 }

func (t int64Type) NewPageBuffer(bufferSize int) PageBuffer { return newInt64PageBuffer(t, bufferSize) }

type int96Type struct{ primitiveType }

func (t int96Type) Kind() Kind { return Int96 }

func (t int96Type) Length() int { return 96 }

func (t int96Type) NewPageBuffer(bufferSize int) PageBuffer { return newInt96PageBuffer(t, bufferSize) }

type floatType struct{ primitiveType }

func (t floatType) Kind() Kind { return Float }

func (t floatType) Length() int { return 32 }

func (t floatType) NewPageBuffer(bufferSize int) PageBuffer { return newFloatPageBuffer(t, bufferSize) }

type doubleType struct{ primitiveType }

func (t doubleType) Kind() Kind { return Double }

func (t doubleType) Length() int { return 64 }

func (t doubleType) NewPageBuffer(bufferSize int) PageBuffer {
	return newDoublePageBuffer(t, bufferSize)
}

type byteArrayType struct{ primitiveType }

func (t byteArrayType) Kind() Kind { return ByteArray }

func (t byteArrayType) Length() int { panic("cannot call Length on parquet binary type") }

func (t byteArrayType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

type fixedLenByteArrayType struct {
	primitiveType
	length int
}

func (t *fixedLenByteArrayType) Kind() Kind { return FixedLenByteArray }

func (t *fixedLenByteArrayType) Length() int { return t.length }

func (t *fixedLenByteArrayType) NewPageBuffer(bufferSize int) PageBuffer {
	return newFixedLenByteArrayPageBuffer(t, bufferSize)
}

var (
	BooleanType   Type = booleanType{}
	Int32Type     Type = int32Type{}
	Int64Type     Type = int64Type{}
	Int96Type     Type = int96Type{}
	FloatType     Type = floatType{}
	DoubleType    Type = doubleType{}
	ByteArrayType Type = byteArrayType{}
)

func FixedLenByteArrayType(length int) Type {
	return &fixedLenByteArrayType{length: length}
}

func Int(bitWidth int) Node {
	return &leafNode{typ: integerType(bitWidth, &signedIntTypes)}
}

func Uint(bitWidth int) Node {
	return &leafNode{typ: integerType(bitWidth, &unsignedIntTypes)}
}

func integerType(bitWidth int, types *[4]intType) *intType {
	switch bitWidth {
	case 8:
		return &types[0]
	case 16:
		return &types[1]
	case 32:
		return &types[2]
	case 64:
		return &types[3]
	default:
		panic(fmt.Sprintf("cannot create a %d bits parquet integer node", bitWidth))
	}
}

var signedIntTypes = [...]intType{
	{BitWidth: 8, IsSigned: true},
	{BitWidth: 16, IsSigned: true},
	{BitWidth: 32, IsSigned: true},
	{BitWidth: 64, IsSigned: true},
}

var unsignedIntTypes = [...]intType{
	{BitWidth: 8, IsSigned: false},
	{BitWidth: 16, IsSigned: false},
	{BitWidth: 32, IsSigned: false},
	{BitWidth: 64, IsSigned: false},
}

type intType format.IntType

func (t *intType) Kind() Kind {
	if t.BitWidth == 64 {
		return Int64
	} else {
		return Int32
	}
}

func (t *intType) Length() int { return int(t.BitWidth) }

func (t *intType) LogicalType() format.LogicalType {
	return format.LogicalType{Integer: (*format.IntType)(t)}
}

func (t *intType) ConvertedType() deprecated.ConvertedType {
	convertedType := int(deprecated.Uint8) + (int(t.BitWidth) / 8)
	if t.IsSigned {
		convertedType += int(deprecated.Int8)
	}
	return deprecated.ConvertedType(convertedType)
}

func (t *intType) NewPageBuffer(bufferSize int) PageBuffer {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newInt64PageBuffer(t, bufferSize)
		} else {
			return newInt32PageBuffer(t, bufferSize)
		}
	} else {
		if t.BitWidth == 64 {
			return uint64PageBuffer{newInt64PageBuffer(t, bufferSize)}
		} else {
			return uint32PageBuffer{newInt32PageBuffer(t, bufferSize)}
		}
	}
}

func Decimal(scale, precision int, typ Type) Node {
	return &leafNode{
		typ: &decimalType{
			decimal: format.DecimalType{
				Scale:     int32(scale),
				Precision: int32(precision),
			},
			typ: typ,
		},
	}
}

type decimalType struct {
	decimal format.DecimalType
	typ     Type
}

func (t *decimalType) Kind() Kind { return t.typ.Kind() }

func (t *decimalType) Length() int { return t.typ.Length() }

func (t *decimalType) LogicalType() format.LogicalType {
	return format.LogicalType{Decimal: &t.decimal}
}

func (t *decimalType) ConvertedType() deprecated.ConvertedType { return deprecated.Decimal }

func (t *decimalType) NewPageBuffer(bufferSize int) PageBuffer { panic("NOT IMPLEMENTED") }

func UTF8() Node { return &leafNode{typ: &stringType{}} }

type stringType format.StringType

func (t *stringType) Kind() Kind { return ByteArray }

func (t *stringType) Length() int { panic("cannot call Length on parquet string type") }

func (t *stringType) LogicalType() format.LogicalType {
	return format.LogicalType{UTF8: (*format.StringType)(t)}
}

func (t *stringType) ConvertedType() deprecated.ConvertedType {
	return deprecated.UTF8
}

func (t *stringType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func UUID() Node { return &leafNode{typ: &uuidType{}} }

type uuidType format.UUIDType

func (t *uuidType) Kind() Kind { return FixedLenByteArray }

func (t *uuidType) Length() int { return 16 }

func (t *uuidType) LogicalType() format.LogicalType {
	return format.LogicalType{UUID: (*format.UUIDType)(t)}
}

func (t *uuidType) ConvertedType() deprecated.ConvertedType { return -1 }

func (t *uuidType) NewPageBuffer(bufferSize int) PageBuffer {
	return uuidPageBuffer{newFixedLenByteArrayPageBuffer(t, bufferSize)}
}

func Enum() Node { return &leafNode{typ: &enumType{}} }

type enumType format.EnumType

func (t *enumType) Kind() Kind { return ByteArray }

func (t *enumType) Length() int { panic("cannot call Length on parquet enum type") }

func (t *enumType) LogicalType() format.LogicalType {
	return format.LogicalType{Enum: (*format.EnumType)(t)}
}

func (t *enumType) ConvertedType() deprecated.ConvertedType { return deprecated.Enum }

func (t *enumType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func JSON() Node { return &leafNode{typ: &jsonType{}} }

type jsonType format.JsonType

func (t *jsonType) Kind() Kind { return ByteArray }

func (t *jsonType) Length() int { panic("cannot call Length on parquet json type") }

func (t *jsonType) LogicalType() format.LogicalType {
	return format.LogicalType{Json: (*format.JsonType)(t)}
}

func (t *jsonType) ConvertedType() deprecated.ConvertedType { return deprecated.Json }

func (t *jsonType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func BSON() Node { return &leafNode{typ: &bsonType{}} }

type bsonType format.BsonType

func (t *bsonType) Kind() Kind { return ByteArray }

func (t *bsonType) Length() int { panic("cannot call Length on parquet bson type") }

func (t *bsonType) LogicalType() format.LogicalType {
	return format.LogicalType{Bson: (*format.BsonType)(t)}
}

func (t *bsonType) ConvertedType() deprecated.ConvertedType { return deprecated.Bson }

func (t *bsonType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func Date() Node { return &leafNode{typ: &dateType{}} }

type dateType format.DateType

func (t *dateType) Kind() Kind { return Int32 }

func (t *dateType) Length() int { return 32 }

func (t *dateType) LogicalType() format.LogicalType {
	return format.LogicalType{Date: (*format.DateType)(t)}
}

func (t *dateType) ConvertedType() deprecated.ConvertedType { return deprecated.Date }

func (t *dateType) NewPageBuffer(bufferSize int) PageBuffer { return newInt32PageBuffer(t, bufferSize) }

type TimeUnit interface {
	Duration() time.Duration

	TimeUnit() format.TimeUnit
}

var (
	Millisecond TimeUnit = &millisecond{}
	Microsecond TimeUnit = &microsecond{}
	Nanosecond  TimeUnit = &nanosecond{}
)

type millisecond format.MilliSeconds

func (u *millisecond) Duration() time.Duration {
	return time.Millisecond
}

func (u *millisecond) TimeUnit() format.TimeUnit {
	return format.TimeUnit{Millis: (*format.MilliSeconds)(u)}
}

type microsecond format.MicroSeconds

func (u *microsecond) Duration() time.Duration {
	return time.Microsecond
}

func (u *microsecond) TimeUnit() format.TimeUnit {
	return format.TimeUnit{Micros: (*format.MicroSeconds)(u)}
}

type nanosecond format.NanoSeconds

func (u *nanosecond) Duration() time.Duration {
	return time.Nanosecond
}

func (u *nanosecond) TimeUnit() format.TimeUnit {
	return format.TimeUnit{Nanos: (*format.NanoSeconds)(u)}
}

func Time(unit TimeUnit) Node {
	return &leafNode{typ: &timeType{IsAdjustedToUTC: true, Unit: unit.TimeUnit()}}
}

type timeType format.TimeType

func (t *timeType) Kind() Kind {
	if t.Unit.Millis != nil {
		return Int32
	} else {
		return Int64
	}
}

func (t *timeType) Length() int {
	if t.Unit.Millis != nil {
		return 32
	} else {
		return 64
	}
}

func (t *timeType) LogicalType() format.LogicalType {
	return format.LogicalType{Time: (*format.TimeType)(t)}
}

func (t *timeType) ConvertedType() deprecated.ConvertedType {
	switch {
	case t.Unit.Millis != nil:
		return deprecated.TimeMillis
	case t.Unit.Micros != nil:
		return deprecated.TimeMicros
	default:
		return -1
	}
}

func (t *timeType) NewPageBuffer(bufferSize int) PageBuffer {
	if t.Unit.Millis != nil {
		return newInt32PageBuffer(t, bufferSize)
	} else {
		return newInt64PageBuffer(t, bufferSize)
	}
}

func Timestamp(unit TimeUnit) Node {
	return &leafNode{typ: &timestampType{IsAdjustedToUTC: true, Unit: unit.TimeUnit()}}
}

type timestampType format.TimestampType

func (t *timestampType) Kind() Kind { return Int64 }

func (t *timestampType) Length() int { return 64 }

func (t *timestampType) LogicalType() format.LogicalType {
	return format.LogicalType{Timestamp: (*format.TimestampType)(t)}
}

func (t *timestampType) ConvertedType() deprecated.ConvertedType {
	switch {
	case t.Unit.Millis != nil:
		return deprecated.TimestampMillis
	case t.Unit.Micros != nil:
		return deprecated.TimestampMicros
	default:
		return -1
	}
}

func (t *timestampType) NewPageBuffer(bufferSize int) PageBuffer {
	return newInt64PageBuffer(t, bufferSize)
}

func List(of Node) Node {
	return listNode{Group{"list": Repeated(Group{"element": of})}}
}

type listNode struct{ Group }

func (listNode) Type() Type { return &listType{} }

type listType format.ListType

func (t *listType) Kind() Kind { panic("cannot call Kind on parquet list type") }

func (t *listType) Length() int { panic("cannot call Length on parquet list type") }

func (t *listType) LogicalType() format.LogicalType {
	return format.LogicalType{List: (*format.ListType)(t)}
}

func (t *listType) ConvertedType() deprecated.ConvertedType { return deprecated.List }

func (t *listType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer for parquet list type")
}

func Map(key, value Node) Node {
	return mapNode{Group{
		"key_value": Repeated(Group{
			"key":   Required(key),
			"value": value,
		}),
	}}
}

type mapNode struct{ Group }

func (mapNode) Type() Type { return &mapType{} }

type mapType format.MapType

func (t *mapType) Kind() Kind { panic("cannot call Kind on parquet map type") }

func (t *mapType) Length() int { panic("cannot call Length on parquet map type") }

func (t *mapType) LogicalType() format.LogicalType {
	return format.LogicalType{Map: (*format.MapType)(t)}
}

func (t *mapType) ConvertedType() deprecated.ConvertedType { return deprecated.Map }

func (t *mapType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer for parquet map type")
}

type nullType format.NullType

func (t *nullType) Kind() Kind { panic("cannot call Kind on null parquet type") }

func (t *nullType) Length() int { panic("cannot call Length on null parquet type") }

func (t *nullType) LogicalType() format.LogicalType {
	return format.LogicalType{Unknown: (*format.NullType)(t)}
}

func (t *nullType) ConvertedType() deprecated.ConvertedType { return -1 }

func (t *nullType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer for parquet null type")
}
