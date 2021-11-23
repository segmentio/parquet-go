package parquet

import (
	"bytes"
	"fmt"
	"time"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type Kind int16

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

	// split Bits + Size
	Length() int

	Less(Value, Value) bool

	PhyiscalType() *format.Type

	LogicalType() *format.LogicalType

	ConvertedType() *deprecated.ConvertedType

	NewDictionary(bufferSize int) Dictionary

	NewPageBuffer(bufferSize int) PageBuffer

	NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader

	NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter
}

var physicalTypes = [...]format.Type{
	0: format.Boolean,
	1: format.Int32,
	2: format.Int64,
	3: format.Int96,
	4: format.Float,
	5: format.Double,
	6: format.ByteArray,
	7: format.FixedLenByteArray,
}

var convertedTypes = [...]deprecated.ConvertedType{
	0:  deprecated.UTF8,
	1:  deprecated.Map,
	2:  deprecated.MapKeyValue,
	3:  deprecated.List,
	4:  deprecated.Enum,
	5:  deprecated.Decimal,
	6:  deprecated.Date,
	7:  deprecated.TimeMillis,
	8:  deprecated.TimeMicros,
	9:  deprecated.TimestampMillis,
	10: deprecated.TimestampMicros,
	11: deprecated.Uint8,
	12: deprecated.Uint16,
	13: deprecated.Uint32,
	14: deprecated.Uint64,
	15: deprecated.Int8,
	16: deprecated.Int16,
	17: deprecated.Int32,
	18: deprecated.Int64,
	19: deprecated.Json,
	20: deprecated.Bson,
	21: deprecated.Interval,
}

// 0-64 of cached type lengths
var typeLengths = [...]int32{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,

	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,

	0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
	0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F,

	0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
	0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,
}

func typeLengthOf(t Type) *int32 {
	switch n := t.Length(); {
	case n == 0:
		return nil
	case n > 0 && n < len(typeLengths):
		return &typeLengths[n]
	default:
		typeLength := int32(n)
		return &typeLength
	}
}

type primitiveType struct{}

func (t primitiveType) LogicalType() *format.LogicalType { return nil }

func (t primitiveType) ConvertedType() *deprecated.ConvertedType { return nil }

type booleanType struct{ primitiveType }

func (t booleanType) Kind() Kind { return Boolean }

func (t booleanType) Length() int { return 1 }

func (t booleanType) PhyiscalType() *format.Type {
	return &physicalTypes[Boolean]
}

func (t booleanType) Less(v1, v2 Value) bool {
	return !v1.Boolean() && v2.Boolean()
}

func (t booleanType) NewDictionary(bufferSize int) Dictionary {
	return newBooleanDictionary(t)
}

func (t booleanType) NewPageBuffer(bufferSize int) PageBuffer {
	return newBooleanPageBuffer(t, bufferSize)
}

func (t booleanType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newBooleanPageReader(t, decoder, bufferSize)
}

func (t booleanType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newBooleanPageWriter(t, encoder, bufferSize)
}

type int32Type struct{ primitiveType }

func (t int32Type) Kind() Kind { return Int32 }

func (t int32Type) Length() int { return 32 }

func (t int32Type) Less(v1, v2 Value) bool {
	return v1.Int32() < v2.Int32()
}

func (t int32Type) PhyiscalType() *format.Type {
	return &physicalTypes[Int32]
}

func (t int32Type) NewDictionary(bufferSize int) Dictionary {
	return newInt32Dictionary(t, bufferSize)
}

func (t int32Type) NewPageBuffer(bufferSize int) PageBuffer {
	return newInt32PageBuffer(t, bufferSize)
}

func (t int32Type) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newInt32PageReader(t, decoder, bufferSize)
}

func (t int32Type) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newInt32PageWriter(t, encoder, bufferSize)
}

type int64Type struct{ primitiveType }

func (t int64Type) Kind() Kind { return Int64 }

func (t int64Type) Length() int { return 64 }

func (t int64Type) Less(v1, v2 Value) bool {
	return v1.Int64() < v2.Int64()
}

func (t int64Type) PhyiscalType() *format.Type {
	return &physicalTypes[Int64]
}

func (t int64Type) NewDictionary(bufferSize int) Dictionary {
	return newInt64Dictionary(t, bufferSize)
}

func (t int64Type) NewPageBuffer(bufferSize int) PageBuffer {
	return newInt64PageBuffer(t, bufferSize)
}

func (t int64Type) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newInt64PageReader(t, decoder, bufferSize)
}

func (t int64Type) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newInt64PageWriter(t, encoder, bufferSize)
}

type int96Type struct{ primitiveType }

func (t int96Type) Kind() Kind { return Int96 }

func (t int96Type) Length() int { return 96 }

func (t int96Type) Less(v1, v2 Value) bool {
	return bits.CompareInt96(v1.Int96(), v2.Int96()) < 0
}

func (t int96Type) PhyiscalType() *format.Type {
	return &physicalTypes[Int96]
}

func (t int96Type) NewDictionary(bufferSize int) Dictionary {
	return newInt96Dictionary(t, bufferSize)
}

func (t int96Type) NewPageBuffer(bufferSize int) PageBuffer {
	return newInt96PageBuffer(t, bufferSize)
}

func (t int96Type) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newInt96PageReader(t, decoder, bufferSize)
}

func (t int96Type) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newInt96PageWriter(t, encoder, bufferSize)
}

type floatType struct{ primitiveType }

func (t floatType) Kind() Kind { return Float }

func (t floatType) Length() int { return 32 }

func (t floatType) Less(v1, v2 Value) bool {
	return v1.Float() < v2.Float()
}

func (t floatType) PhyiscalType() *format.Type {
	return &physicalTypes[Float]
}

func (t floatType) NewDictionary(bufferSize int) Dictionary {
	return newFloatDictionary(t, bufferSize)
}

func (t floatType) NewPageBuffer(bufferSize int) PageBuffer {
	return newFloatPageBuffer(t, bufferSize)
}

func (t floatType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newFloatPageReader(t, decoder, bufferSize)
}

func (t floatType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newFloatPageWriter(t, encoder, bufferSize)
}

type doubleType struct{ primitiveType }

func (t doubleType) Kind() Kind { return Double }

func (t doubleType) Length() int { return 64 }

func (t doubleType) Less(v1, v2 Value) bool { return v1.Double() < v2.Double() }

func (t doubleType) PhyiscalType() *format.Type { return &physicalTypes[Double] }

func (t doubleType) NewDictionary(bufferSize int) Dictionary {
	return newDoubleDictionary(t, bufferSize)
}

func (t doubleType) NewPageBuffer(bufferSize int) PageBuffer {
	return newDoublePageBuffer(t, bufferSize)
}

func (t doubleType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newDoublePageReader(t, decoder, bufferSize)
}

func (t doubleType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newDoublePageWriter(t, encoder, bufferSize)
}

type byteArrayType struct{ primitiveType }

func (t byteArrayType) Kind() Kind { return ByteArray }

func (t byteArrayType) Length() int { return 0 }

func (t byteArrayType) Less(v1, v2 Value) bool {
	return bytes.Compare(v1.ByteArray(), v2.ByteArray()) < 0
}

func (t byteArrayType) PhyiscalType() *format.Type { return &physicalTypes[ByteArray] }

func (t byteArrayType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t byteArrayType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func (t byteArrayType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newByteArrayPageReader(t, decoder, bufferSize)
}

func (t byteArrayType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newByteArrayPageWriter(t, encoder, bufferSize)
}

type fixedLenByteArrayType struct {
	primitiveType
	length int
}

func (t *fixedLenByteArrayType) Kind() Kind { return FixedLenByteArray }

func (t *fixedLenByteArrayType) Length() int { return t.length }

func (t *fixedLenByteArrayType) Less(v1, v2 Value) bool {
	return bytes.Compare(v1.ByteArray(), v2.ByteArray()) < 0
}

func (t *fixedLenByteArrayType) PhyiscalType() *format.Type {
	return &physicalTypes[FixedLenByteArray]
}

func (t *fixedLenByteArrayType) NewDictionary(bufferSize int) Dictionary {
	return newFixedLenByteArrayDictionary(t, bufferSize)
}

func (t *fixedLenByteArrayType) NewPageBuffer(bufferSize int) PageBuffer {
	return newFixedLenByteArrayPageBuffer(t, bufferSize)
}

func (t *fixedLenByteArrayType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newFixedLenByteArrayPageReader(t, decoder, bufferSize)
}

func (t *fixedLenByteArrayType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newFixedLenByteArrayPageWriter(t, encoder, bufferSize)
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

func (t *intType) Less(v1, v2 Value) bool {
	if t.BitWidth == 64 {
		i1 := v1.Int64()
		i2 := v2.Int64()
		if t.IsSigned {
			return i1 < i2
		} else {
			return uint64(i1) < uint64(i2)
		}
	} else {
		i1 := v1.Int32()
		i2 := v2.Int32()
		if t.IsSigned {
			return i1 < i2
		} else {
			return uint32(i1) < uint32(i2)
		}
	}
}

func (t *intType) PhyiscalType() *format.Type {
	if t.BitWidth == 64 {
		return &physicalTypes[Int64]
	} else {
		return &physicalTypes[Int32]
	}
}

func (t *intType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Integer: (*format.IntType)(t)}
}

func (t *intType) ConvertedType() *deprecated.ConvertedType {
	convertedType := int(deprecated.Uint8) + (int(t.BitWidth) / 8)
	if t.IsSigned {
		convertedType += int(deprecated.Int8)
	}
	return &convertedTypes[convertedType]
}

func (t *intType) NewDictionary(bufferSize int) Dictionary {
	if t.BitWidth == 64 {
		return newInt64Dictionary(t, bufferSize)
	} else {
		return newInt32Dictionary(t, bufferSize)
	}
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

func (t *intType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	if t.BitWidth == 64 {
		return newInt64PageReader(t, decoder, bufferSize)
	} else {
		return newInt32PageReader(t, decoder, bufferSize)
	}
}

func (t *intType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newInt64PageWriter(t, encoder, bufferSize)
		} else {
			return newInt32PageWriter(t, encoder, bufferSize)
		}
	} else {
		if t.BitWidth == 64 {
			return newUint64PageWriter(t, encoder, bufferSize)
		} else {
			return newUint32PageWriter(t, encoder, bufferSize)
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

func (t *decimalType) PhyiscalType() *format.Type { return t.typ.PhyiscalType() }

func (t *decimalType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Decimal: &t.decimal}
}

func (t *decimalType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Decimal]
}

func (t *decimalType) Less(v1, v2 Value) bool { panic("NOT IMPLEMENTED") }

func (t *decimalType) NewDictionary(bufferSize int) Dictionary { panic("NOT IMPLEMENTED") }

func (t *decimalType) NewPageBuffer(bufferSize int) PageBuffer { panic("NOT IMPLEMENTED") }

func (t *decimalType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	panic("NOT IMPLEMENTED")
}

func (t *decimalType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	panic("NOT IMPLEMENTED")
}

func String() Node { return &leafNode{typ: &stringType{}} }

type stringType format.StringType

func (t *stringType) Kind() Kind { return ByteArray }

func (t *stringType) Length() int { return 0 }

func (t *stringType) Less(v1, v2 Value) bool {
	return bytes.Compare(v1.ByteArray(), v2.ByteArray()) < 0
}

func (t *stringType) PhyiscalType() *format.Type {
	return &physicalTypes[ByteArray]
}

func (t *stringType) LogicalType() *format.LogicalType {
	return &format.LogicalType{UTF8: (*format.StringType)(t)}
}

func (t *stringType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.UTF8]
}

func (t *stringType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t *stringType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func (t *stringType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newByteArrayPageReader(t, decoder, bufferSize)
}

func (t *stringType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newByteArrayPageWriter(t, encoder, bufferSize)
}

func UUID() Node { return &leafNode{typ: &uuidType{}} }

type uuidType format.UUIDType

func (t *uuidType) Kind() Kind { return FixedLenByteArray }

func (t *uuidType) Length() int { return 16 }

func (t *uuidType) Less(v1, v2 Value) bool {
	return bytes.Compare(v1.ByteArray(), v2.ByteArray()) < 0
}

func (t *uuidType) PhyiscalType() *format.Type {
	return &physicalTypes[ByteArray]
}

func (t *uuidType) LogicalType() *format.LogicalType {
	return &format.LogicalType{UUID: (*format.UUIDType)(t)}
}

func (t *uuidType) ConvertedType() *deprecated.ConvertedType { return nil }

func (t *uuidType) NewDictionary(bufferSize int) Dictionary {
	return uuidDictionary{newFixedLenByteArrayDictionary(t, bufferSize)}
}

func (t *uuidType) NewPageBuffer(bufferSize int) PageBuffer {
	return uuidPageBuffer{newFixedLenByteArrayPageBuffer(t, bufferSize)}
}

func (t *uuidType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newFixedLenByteArrayPageReader(t, decoder, bufferSize)
}

func (t *uuidType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return uuidPageWriter{newFixedLenByteArrayPageWriter(t, encoder, bufferSize)}
}

func Enum() Node { return &leafNode{typ: &enumType{}} }

type enumType format.EnumType

func (t *enumType) Kind() Kind { return ByteArray }

func (t *enumType) Length() int { return 0 }

func (t *enumType) Less(v1, v2 Value) bool {
	return bytes.Compare(v1.ByteArray(), v2.ByteArray()) < 0
}

func (t *enumType) PhyiscalType() *format.Type {
	return &physicalTypes[ByteArray]
}

func (t *enumType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Enum: (*format.EnumType)(t)}
}

func (t *enumType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Enum]
}

func (t *enumType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t *enumType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func (t *enumType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newByteArrayPageReader(t, decoder, bufferSize)
}

func (t *enumType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newByteArrayPageWriter(t, encoder, bufferSize)
}

func JSON() Node { return &leafNode{typ: &jsonType{}} }

type jsonType format.JsonType

func (t *jsonType) Kind() Kind { return ByteArray }

func (t *jsonType) Length() int { return 0 }

func (t *jsonType) Less(v1, v2 Value) bool {
	return bytes.Compare(v1.ByteArray(), v2.ByteArray()) < 0
}

func (t *jsonType) PhyiscalType() *format.Type {
	return &physicalTypes[ByteArray]
}

func (t *jsonType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Json: (*format.JsonType)(t)}
}

func (t *jsonType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Json]
}

func (t *jsonType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t *jsonType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func (t *jsonType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newByteArrayPageReader(t, decoder, bufferSize)
}

func (t *jsonType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newByteArrayPageWriter(t, encoder, bufferSize)
}

func BSON() Node { return &leafNode{typ: &bsonType{}} }

type bsonType format.BsonType

func (t *bsonType) Kind() Kind { return ByteArray }

func (t *bsonType) Length() int { return 0 }

func (t *bsonType) Less(v1, v2 Value) bool {
	return bytes.Compare(v1.ByteArray(), v2.ByteArray()) < 0
}

func (t *bsonType) PhyiscalType() *format.Type {
	return &physicalTypes[ByteArray]
}

func (t *bsonType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Bson: (*format.BsonType)(t)}
}

func (t *bsonType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Bson]
}

func (t *bsonType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t *bsonType) NewPageBuffer(bufferSize int) PageBuffer {
	return newByteArrayPageBuffer(t, bufferSize)
}

func (t *bsonType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newByteArrayPageReader(t, decoder, bufferSize)
}

func (t *bsonType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newByteArrayPageWriter(t, encoder, bufferSize)
}

func Date() Node { return &leafNode{typ: &dateType{}} }

type dateType format.DateType

func (t *dateType) Kind() Kind { return Int32 }

func (t *dateType) Length() int { return 32 }

func (t *dateType) Less(v1, v2 Value) bool { return v1.Int32() < v2.Int32() }

func (t *dateType) PhyiscalType() *format.Type { return &physicalTypes[Int32] }

func (t *dateType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Date: (*format.DateType)(t)}
}

func (t *dateType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Date]
}

func (t *dateType) NewDictionary(bufferSize int) Dictionary {
	return newInt32Dictionary(t, bufferSize)
}

func (t *dateType) NewPageBuffer(bufferSize int) PageBuffer {
	return newInt32PageBuffer(t, bufferSize)
}

func (t *dateType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newInt32PageReader(t, decoder, bufferSize)
}

func (t *dateType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newInt32PageWriter(t, encoder, bufferSize)
}

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

func (u *millisecond) Duration() time.Duration { return time.Millisecond }
func (u *millisecond) TimeUnit() format.TimeUnit {
	return format.TimeUnit{Millis: (*format.MilliSeconds)(u)}
}

type microsecond format.MicroSeconds

func (u *microsecond) Duration() time.Duration { return time.Microsecond }
func (u *microsecond) TimeUnit() format.TimeUnit {
	return format.TimeUnit{Micros: (*format.MicroSeconds)(u)}
}

type nanosecond format.NanoSeconds

func (u *nanosecond) Duration() time.Duration { return time.Nanosecond }
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

func (t *timeType) Less(v1, v2 Value) bool {
	if t.Unit.Millis != nil {
		return v1.Int32() < v2.Int32()
	} else {
		return v1.Int64() < v2.Int64()
	}
}

func (t *timeType) PhyiscalType() *format.Type {
	if t.Unit.Millis != nil {
		return &physicalTypes[Int32]
	} else {
		return &physicalTypes[Int64]
	}
}

func (t *timeType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Time: (*format.TimeType)(t)}
}

func (t *timeType) ConvertedType() *deprecated.ConvertedType {
	switch {
	case t.Unit.Millis != nil:
		return &convertedTypes[deprecated.TimeMillis]
	case t.Unit.Micros != nil:
		return &convertedTypes[deprecated.TimeMicros]
	default:
		return nil
	}
}

func (t *timeType) NewDictionary(bufferSize int) Dictionary {
	if t.Unit.Millis != nil {
		return newInt32Dictionary(t, bufferSize)
	} else {
		return newInt64Dictionary(t, bufferSize)
	}
}

func (t *timeType) NewPageBuffer(bufferSize int) PageBuffer {
	if t.Unit.Millis != nil {
		return newInt32PageBuffer(t, bufferSize)
	} else {
		return newInt64PageBuffer(t, bufferSize)
	}
}

func (t *timeType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	if t.Unit.Millis != nil {
		return newInt32PageReader(t, decoder, bufferSize)
	} else {
		return newInt64PageReader(t, decoder, bufferSize)
	}
}

func (t *timeType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	if t.Unit.Millis != nil {
		return newInt32PageWriter(t, encoder, bufferSize)
	} else {
		return newInt64PageWriter(t, encoder, bufferSize)
	}
}

func Timestamp(unit TimeUnit) Node {
	return &leafNode{typ: &timestampType{IsAdjustedToUTC: true, Unit: unit.TimeUnit()}}
}

type timestampType format.TimestampType

func (t *timestampType) Kind() Kind { return Int64 }

func (t *timestampType) Length() int { return 64 }

func (t *timestampType) Less(v1, v2 Value) bool { return v1.Int64() < v2.Int64() }

func (t *timestampType) PhyiscalType() *format.Type { return &physicalTypes[Int64] }

func (t *timestampType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Timestamp: (*format.TimestampType)(t)}
}

func (t *timestampType) ConvertedType() *deprecated.ConvertedType {
	switch {
	case t.Unit.Millis != nil:
		return &convertedTypes[deprecated.TimestampMillis]
	case t.Unit.Micros != nil:
		return &convertedTypes[deprecated.TimestampMicros]
	default:
		return nil
	}
}

func (t *timestampType) NewDictionary(bufferSize int) Dictionary {
	return newInt64Dictionary(t, bufferSize)
}

func (t *timestampType) NewPageBuffer(bufferSize int) PageBuffer {
	return newInt64PageBuffer(t, bufferSize)
}

func (t *timestampType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	return newInt64PageReader(t, decoder, bufferSize)
}

func (t *timestampType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	return newInt64PageWriter(t, encoder, bufferSize)
}

func List(of Node) Node {
	return listNode{Group{"list": Repeated(Group{"element": of})}}
}

type listNode struct{ Group }

func (listNode) Type() Type { return &listType{} }

type listType format.ListType

func (t *listType) Kind() Kind { panic("cannot call Kind on parquet LIST type") }

func (t *listType) Length() int { return 0 }

func (t *listType) Less(Value, Value) bool { panic("cannot compare values on parquet LIST type") }

func (t *listType) PhyiscalType() *format.Type { return nil }

func (t *listType) LogicalType() *format.LogicalType {
	return &format.LogicalType{List: (*format.ListType)(t)}
}

func (t *listType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.List]
}

func (t *listType) NewDictionary(bufferSize int) Dictionary {
	panic("cannot create dictionary from parquet LIST type")
}

func (t *listType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer from parquet LIST type")
}

func (t *listType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	panic("cannot create page reader from parquet LIST type")
}

func (t *listType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	panic("cannot create page writer from parquet LIST type")
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

func (t *mapType) Kind() Kind { panic("cannot call Kind on parquet MAP type") }

func (t *mapType) Length() int { return 0 }

func (t *mapType) Less(Value, Value) bool { panic("cannot compare values on parquet MAP type") }

func (t *mapType) PhyiscalType() *format.Type { return nil }

func (t *mapType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Map: (*format.MapType)(t)}
}

func (t *mapType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Map]
}

func (t *mapType) NewDictionary(bufferSize int) Dictionary {
	panic("cannot create dictionary from parquet MAP type")
}

func (t *mapType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer from parquet MAP type")
}

func (t *mapType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	panic("cannot create page reader from parquet MAP type")
}

func (t *mapType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	panic("cannot create page writer from parquet MAP type")
}

type nullType format.NullType

func (t *nullType) Kind() Kind { panic("cannot call Kind on parquet NULL type") }

func (t *nullType) Length() int { return 0 }

func (t *nullType) Less(Value, Value) bool { panic("cannot compare values on parquet NULL type") }

func (t *nullType) PhyiscalType() *format.Type { return nil }

func (t *nullType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Unknown: (*format.NullType)(t)}
}

func (t *nullType) ConvertedType() *deprecated.ConvertedType { return nil }

func (t *nullType) NewDictionary(bufferSize int) Dictionary {
	panic("cannot create dictionary for parquet NULL type")
}

func (t *nullType) NewPageBuffer(bufferSize int) PageBuffer {
	panic("cannot create page buffer for parquet NULL type")
}

func (t *nullType) NewPageReader(decoder encoding.Decoder, bufferSize int) PageReader {
	panic("cannot create page reader for parquet NULL type")
}

func (t *nullType) NewPageWriter(encoder encoding.Encoder, bufferSize int) PageWriter {
	panic("cannot create page writer for parquet NULL type")
}
