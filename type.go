package parquet

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

// Kind is an enumeration type representing the physical types supported by the
// parquet type system.
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

// String returns a human-readable representation of the physical type.
func (k Kind) String() string { return format.Type(k).String() }

// The Type interface represents logical types of the parquet type system.
//
// Types are immutable and therefore safe to access from multiple goroutines.
type Type interface {
	// Returns a human-redable representation of the parquet type.
	String() string

	// Returns the Kind value representing the underlying physical type.
	//
	// The method panics if it is called on a group type.
	Kind() Kind

	// For integer and floating point physical types, the method returns the
	// size of values in bits.
	//
	// For fixed-length byte arrays, the method returns the size of elements
	// in bytes.
	//
	// For other types, the value is zero.
	Length() int

	// Compares two values and returns a negative value if a < b, positive if
	// a > b, or zero if a == b.
	//
	// The values Kind must match the type, otherwise the result is undefined.
	//
	// The method panics if it is called on a group type.
	Compare(a, b Value) int

	// ColumnOrder returns the type's column order. For group types, this method
	// returns nil.
	//
	// The order describes the comprison logic implemented by the Less method.
	//
	// As an optimization, the method may return the same pointer across
	// multiple calls. Applications must treat the returned value as immutable,
	// mutating the value will result in undefined behavior.
	ColumnOrder() *format.ColumnOrder

	// Returns the physical type as a *format.Type value. For group types, this
	// method returns nil.
	//
	// As an optimization, the method may return the same pointer across
	// multiple calls. Applications must treat the returned value as immutable,
	// mutating the value will result in undefined behavior.
	PhyiscalType() *format.Type

	// Returns the logical type as a *format.LogicalType value. When the logical
	// type is unknown, the method returns nil.
	//
	// As an optimization, the method may return the same pointer across
	// multiple calls. Applications must treat the returned value as immutable,
	// mutating the value will result in undefined behavior.
	LogicalType() *format.LogicalType

	// Returns the logical type's equivaleent converted type. When there are
	// no equivalent converted type, the method returns nil.
	//
	// As an optimization, the method may return the same pointer across
	// multiple calls. Applications must treat the returned value as immutable,
	// mutating the value will result in undefined behavior.
	ConvertedType() *deprecated.ConvertedType

	// Creates a column indexer for values of this type.
	//
	// The size limit is a hint to the column indexer that it is allowed to
	// truncate the page boundaries to the given size. Only BYTE_ARRAY and
	// FIXED_LEN_BYTE_ARRAY types currently take this value into account.
	//
	// A value of zero or less means no limits.
	//
	// The method panics if it is called on a group type.
	NewColumnIndexer(sizeLimit int) ColumnIndexer

	// Creates a dictionary holding values of this type.
	//
	// The method panics if it is called on a group type.
	NewDictionary(bufferSize int) Dictionary

	// Creates a row group buffer column for values of this type.
	//
	// The method panics if it is called on a group type.
	NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer

	// Creates a decoder for values of this type.
	//
	// The method panics if it is called on a group type.
	NewValueDecoder(bufferSize int) ValueDecoder
}

// In the current parquet version supported by this library, only type-defined
// orders are supported.
var typeDefinedColumnOrder = format.ColumnOrder{
	TypeOrder: new(format.TypeDefinedOrder),
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

type primitiveType struct{}

func (t primitiveType) ColumnOrder() *format.ColumnOrder { return &typeDefinedColumnOrder }

func (t primitiveType) LogicalType() *format.LogicalType { return nil }

func (t primitiveType) ConvertedType() *deprecated.ConvertedType { return nil }

type booleanType struct{ primitiveType }

func (t booleanType) String() string { return "BOOLEAN" }

func (t booleanType) Kind() Kind { return Boolean }

func (t booleanType) Length() int { return 1 }

func (t booleanType) Compare(a, b Value) int {
	return compareBool(a.Boolean(), b.Boolean())
}

func (t booleanType) PhyiscalType() *format.Type {
	return &physicalTypes[Boolean]
}

func (t booleanType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newBooleanColumnIndexer()
}

func (t booleanType) NewDictionary(bufferSize int) Dictionary {
	return newBooleanDictionary(t)
}

func (t booleanType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newBooleanColumnBuffer(columnIndex, bufferSize)
}

func (t booleanType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newBooleanValueDecoder(bufferSize)
}

type int32Type struct{ primitiveType }

func (t int32Type) String() string { return "INT32" }

func (t int32Type) Kind() Kind { return Int32 }

func (t int32Type) Length() int { return 32 }

func (t int32Type) Compare(a, b Value) int {
	return compareInt32(a.Int32(), b.Int32())
}

func (t int32Type) PhyiscalType() *format.Type {
	return &physicalTypes[Int32]
}

func (t int32Type) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt32ColumnIndexer()
}

func (t int32Type) NewDictionary(bufferSize int) Dictionary {
	return newInt32Dictionary(t, bufferSize)
}

func (t int32Type) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt32ColumnBuffer(columnIndex, bufferSize)
}

func (t int32Type) NewValueDecoder(bufferSize int) ValueDecoder {
	return newInt32ValueDecoder(bufferSize)
}

type int64Type struct{ primitiveType }

func (t int64Type) String() string { return "INT64" }

func (t int64Type) Kind() Kind { return Int64 }

func (t int64Type) Length() int { return 64 }

func (t int64Type) Compare(a, b Value) int {
	return compareInt64(a.Int64(), b.Int64())
}

func (t int64Type) PhyiscalType() *format.Type {
	return &physicalTypes[Int64]
}

func (t int64Type) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt64ColumnIndexer()
}

func (t int64Type) NewDictionary(bufferSize int) Dictionary {
	return newInt64Dictionary(t, bufferSize)
}

func (t int64Type) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt64ColumnBuffer(columnIndex, bufferSize)
}

func (t int64Type) NewValueDecoder(bufferSize int) ValueDecoder {
	return newInt64ValueDecoder(bufferSize)
}

type int96Type struct{ primitiveType }

func (t int96Type) String() string { return "INT96" }

func (t int96Type) Kind() Kind { return Int96 }

func (t int96Type) Length() int { return 96 }

func (t int96Type) Compare(a, b Value) int {
	return compareInt96(a.Int96(), b.Int96())
}

func (t int96Type) PhyiscalType() *format.Type {
	return &physicalTypes[Int96]
}

func (t int96Type) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt96ColumnIndexer()
}

func (t int96Type) NewDictionary(bufferSize int) Dictionary {
	return newInt96Dictionary(t, bufferSize)
}

func (t int96Type) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt96ColumnBuffer(columnIndex, bufferSize)
}

func (t int96Type) NewValueDecoder(bufferSize int) ValueDecoder {
	return newInt96ValueDecoder(bufferSize)
}

type floatType struct{ primitiveType }

func (t floatType) String() string { return "FLOAT" }

func (t floatType) Kind() Kind { return Float }

func (t floatType) Length() int { return 32 }

func (t floatType) Compare(a, b Value) int {
	return compareFloat32(a.Float(), b.Float())
}

func (t floatType) PhyiscalType() *format.Type {
	return &physicalTypes[Float]
}

func (t floatType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newFloatColumnIndexer()
}

func (t floatType) NewDictionary(bufferSize int) Dictionary {
	return newFloatDictionary(t, bufferSize)
}

func (t floatType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newFloatColumnBuffer(columnIndex, bufferSize)
}

func (t floatType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newFloatValueDecoder(bufferSize)
}

type doubleType struct{ primitiveType }

func (t doubleType) String() string { return "DOUBLE" }

func (t doubleType) Kind() Kind { return Double }

func (t doubleType) Length() int { return 64 }

func (t doubleType) Compare(a, b Value) int {
	return compareFloat64(a.Double(), b.Double())
}

func (t doubleType) PhyiscalType() *format.Type { return &physicalTypes[Double] }

func (t doubleType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newDoubleColumnIndexer()
}

func (t doubleType) NewDictionary(bufferSize int) Dictionary {
	return newDoubleDictionary(t, bufferSize)
}

func (t doubleType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newDoubleColumnBuffer(columnIndex, bufferSize)
}

func (t doubleType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newDoubleValueDecoder(bufferSize)
}

type byteArrayType struct{ primitiveType }

func (t byteArrayType) String() string { return "BYTE_ARRAY" }

func (t byteArrayType) Kind() Kind { return ByteArray }

func (t byteArrayType) Length() int { return 0 }

func (t byteArrayType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t byteArrayType) PhyiscalType() *format.Type {
	return &physicalTypes[ByteArray]
}

func (t byteArrayType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newByteArrayColumnIndexer(sizeLimit)
}

func (t byteArrayType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t byteArrayType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newByteArrayColumnBuffer(columnIndex, bufferSize)
}

func (t byteArrayType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newByteArrayValueDecoder(bufferSize)
}

type fixedLenByteArrayType struct {
	primitiveType
	length int
}

func (t *fixedLenByteArrayType) String() string {
	return fmt.Sprintf("FIXED_LEN_BYTE_ARRAY(%d)", t.length)
}

func (t *fixedLenByteArrayType) Kind() Kind { return FixedLenByteArray }

func (t *fixedLenByteArrayType) Length() int { return t.length }

func (t *fixedLenByteArrayType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t *fixedLenByteArrayType) PhyiscalType() *format.Type {
	return &physicalTypes[FixedLenByteArray]
}

func (t *fixedLenByteArrayType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newFixedLenByteArrayColumnIndexer(t.length, sizeLimit)
}

func (t *fixedLenByteArrayType) NewDictionary(bufferSize int) Dictionary {
	return newFixedLenByteArrayDictionary(t, bufferSize)
}

func (t *fixedLenByteArrayType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newFixedLenByteArrayColumnBuffer(t.length, columnIndex, bufferSize)
}

func (t *fixedLenByteArrayType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newFixedLenByteArrayValueDecoder(t.length, bufferSize)
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

// FixedLenByteArrayType constructs a type for fixed-length values of the given
// size (in bytes).
func FixedLenByteArrayType(length int) Type {
	return &fixedLenByteArrayType{length: length}
}

// Int constructs a leaf node of signed integer logical type of the given bit
// width.
//
// The bit width must be one of 8, 16, 32, 64, or the function will panic.
func Int(bitWidth int) Node {
	return Leaf(integerType(bitWidth, &signedIntTypes))
}

// Uint constructts a leaf node of unsigned integer logical type of the given
// bit width.
//
// The bit width must be one of 8, 16, 32, 64, or the function will panic.
func Uint(bitWidth int) Node {
	return Leaf(integerType(bitWidth, &unsignedIntTypes))
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

func (t *intType) String() string { return (*format.IntType)(t).String() }

func (t *intType) Kind() Kind {
	if t.BitWidth == 64 {
		return Int64
	} else {
		return Int32
	}
}

func (t *intType) Length() int { return int(t.BitWidth) }

func (t *intType) Compare(a, b Value) int {
	if t.BitWidth == 64 {
		i1 := a.Int64()
		i2 := b.Int64()
		if t.IsSigned {
			return compareInt64(i1, i2)
		} else {
			return compareUint64(uint64(i1), uint64(i2))
		}
	} else {
		i1 := a.Int32()
		i2 := b.Int32()
		if t.IsSigned {
			return compareInt32(i1, i2)
		} else {
			return compareUint32(uint32(i1), uint32(i2))
		}
	}
}

func (t *intType) ColumnOrder() *format.ColumnOrder {
	return &typeDefinedColumnOrder
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
	convertedType := bits.Len8(int8(t.BitWidth)/8) - 1 // 8=>0, 16=>1, 32=>2, 64=>4
	if t.IsSigned {
		convertedType += int(deprecated.Int8)
	} else {
		convertedType += int(deprecated.Uint8)
	}
	return &convertedTypes[convertedType]
}

func (t *intType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newInt64ColumnIndexer()
		} else {
			return newInt32ColumnIndexer()
		}
	} else {
		if t.BitWidth == 64 {
			return newUint64ColumnIndexer()
		} else {
			return newUint32ColumnIndexer()
		}
	}
}

func (t *intType) NewDictionary(bufferSize int) Dictionary {
	if t.BitWidth == 64 {
		return newInt64Dictionary(t, bufferSize)
	} else {
		return newInt32Dictionary(t, bufferSize)
	}
}

func (t *intType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newInt64ColumnBuffer(columnIndex, bufferSize)
		} else {
			return newInt32ColumnBuffer(columnIndex, bufferSize)
		}
	} else {
		if t.BitWidth == 64 {
			return newUint64ColumnBuffer(columnIndex, bufferSize)
		} else {
			return newUint32ColumnBuffer(columnIndex, bufferSize)
		}
	}
}

func (t *intType) NewValueDecoder(bufferSize int) ValueDecoder {
	if t.BitWidth == 64 {
		return newInt64ValueDecoder(bufferSize)
	} else {
		return newInt32ValueDecoder(bufferSize)
	}
}

// Decimal constructs a leaf node of decimal logical ttype with the given
// sccale, precision, and underlying type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
func Decimal(scale, precision int, typ Type) Node {
	switch typ {
	case Int32Type, Int64Type:
	default:
		panic("DECIMAL node must annotate the INT32 or INT64 types but got " + typ.String())
	}
	return Leaf(&decimalType{
		decimal: format.DecimalType{
			Scale:     int32(scale),
			Precision: int32(precision),
		},
		Type: typ,
	})
}

type decimalType struct {
	decimal format.DecimalType
	Type
}

func (t *decimalType) String() string { return t.decimal.String() }

func (t *decimalType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Decimal: &t.decimal}
}

func (t *decimalType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Decimal]
}

// String constructs a leaf node of UTF8 logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#string
func String() Node { return Leaf(&stringType{}) }

type stringType format.StringType

func (t *stringType) String() string { return (*format.StringType)(t).String() }

func (t *stringType) Kind() Kind { return ByteArray }

func (t *stringType) Length() int { return 0 }

func (t *stringType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t *stringType) ColumnOrder() *format.ColumnOrder {
	return &typeDefinedColumnOrder
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

func (t *stringType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newByteArrayColumnIndexer(sizeLimit)
}

func (t *stringType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t *stringType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newByteArrayColumnBuffer(columnIndex, bufferSize)
}

func (t *stringType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newByteArrayValueDecoder(bufferSize)
}

func (t *stringType) GoType() reflect.Type {
	return reflect.TypeOf("")
}

// UUID constructs a leaf node of UUID logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#uuid
func UUID() Node { return Leaf(&uuidType{}) }

type uuidType format.UUIDType

func (t *uuidType) String() string { return (*format.UUIDType)(t).String() }

func (t *uuidType) Kind() Kind { return FixedLenByteArray }

func (t *uuidType) Length() int { return 16 }

func (t *uuidType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t *uuidType) ColumnOrder() *format.ColumnOrder {
	return &typeDefinedColumnOrder
}

func (t *uuidType) PhyiscalType() *format.Type {
	return &physicalTypes[ByteArray]
}

func (t *uuidType) LogicalType() *format.LogicalType {
	return &format.LogicalType{UUID: (*format.UUIDType)(t)}
}

func (t *uuidType) ConvertedType() *deprecated.ConvertedType { return nil }

func (t *uuidType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newFixedLenByteArrayColumnIndexer(16, sizeLimit)
}

func (t *uuidType) NewDictionary(bufferSize int) Dictionary {
	return newFixedLenByteArrayDictionary(t, bufferSize)
}

func (t *uuidType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newFixedLenByteArrayColumnBuffer(16, columnIndex, bufferSize)
}

func (t *uuidType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newFixedLenByteArrayValueDecoder(16, bufferSize)
}

func (t *uuidType) GoType() reflect.Type {
	return reflect.TypeOf(uuid.UUID{})
}

// Enum constructs a leaf node with a logical type representing enumerations.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#enum
func Enum() Node { return Leaf(&enumType{}) }

type enumType format.EnumType

func (t *enumType) String() string { return (*format.EnumType)(t).String() }

func (t *enumType) Kind() Kind { return ByteArray }

func (t *enumType) Length() int { return 0 }

func (t *enumType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t *enumType) ColumnOrder() *format.ColumnOrder {
	return &typeDefinedColumnOrder
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

func (t *enumType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newByteArrayColumnIndexer(sizeLimit)
}

func (t *enumType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t *enumType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newByteArrayColumnBuffer(columnIndex, bufferSize)
}

func (t *enumType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newByteArrayValueDecoder(bufferSize)
}

func (t *enumType) GoType() reflect.Type {
	return reflect.TypeOf("")
}

// JSON constructs a leaf node of JSON logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#json
func JSON() Node { return Leaf(&jsonType{}) }

type jsonType format.JsonType

func (t *jsonType) String() string { return (*jsonType)(t).String() }

func (t *jsonType) Kind() Kind { return ByteArray }

func (t *jsonType) Length() int { return 0 }

func (t *jsonType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t *jsonType) ColumnOrder() *format.ColumnOrder {
	return &typeDefinedColumnOrder
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

func (t *jsonType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newByteArrayColumnIndexer(sizeLimit)
}

func (t *jsonType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t *jsonType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newByteArrayColumnBuffer(columnIndex, bufferSize)
}

func (t *jsonType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newByteArrayValueDecoder(bufferSize)
}

// BSON constructs a leaf node of BSON logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#bson
func BSON() Node { return Leaf(&bsonType{}) }

type bsonType format.BsonType

func (t *bsonType) String() string { return (*format.BsonType)(t).String() }

func (t *bsonType) Kind() Kind { return ByteArray }

func (t *bsonType) Length() int { return 0 }

func (t *bsonType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t *bsonType) ColumnOrder() *format.ColumnOrder {
	return &typeDefinedColumnOrder
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

func (t *bsonType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newByteArrayColumnIndexer(sizeLimit)
}

func (t *bsonType) NewDictionary(bufferSize int) Dictionary {
	return newByteArrayDictionary(t, bufferSize)
}

func (t *bsonType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newByteArrayColumnBuffer(columnIndex, bufferSize)
}

func (t *bsonType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newByteArrayValueDecoder(bufferSize)
}

// Date constructs a leaf node of DATE logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date
func Date() Node { return Leaf(&dateType{}) }

type dateType format.DateType

func (t *dateType) String() string { return (*format.DateType)(t).String() }

func (t *dateType) Kind() Kind { return Int32 }

func (t *dateType) Length() int { return 32 }

func (t *dateType) Compare(a, b Value) int { return compareInt32(a.Int32(), b.Int32()) }

func (t *dateType) ColumnOrder() *format.ColumnOrder {
	return &typeDefinedColumnOrder
}

func (t *dateType) PhyiscalType() *format.Type { return &physicalTypes[Int32] }

func (t *dateType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Date: (*format.DateType)(t)}
}

func (t *dateType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Date]
}

func (t *dateType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt32ColumnIndexer()
}

func (t *dateType) NewDictionary(bufferSize int) Dictionary {
	return newInt32Dictionary(t, bufferSize)
}

func (t *dateType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt32ColumnBuffer(columnIndex, bufferSize)
}

func (t *dateType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newInt32ValueDecoder(bufferSize)
}

// TimeUnit represents units of time in the parquet type system.
type TimeUnit interface {
	// Returns the precision of the time unit as a time.Duration value.
	Duration() time.Duration
	// Converts the TimeUnit value to its representation in the parquet thrift
	// format.
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

// Time constructs a leaf node of TIME logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time
func Time(unit TimeUnit) Node {
	return Leaf(&timeType{IsAdjustedToUTC: true, Unit: unit.TimeUnit()})
}

type timeType format.TimeType

func (t *timeType) String() string {
	return (*format.TimeType)(t).String()
}

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

func (t *timeType) Compare(a, b Value) int {
	if t.Unit.Millis != nil {
		return compareInt32(a.Int32(), b.Int32())
	} else {
		return compareInt64(a.Int64(), b.Int64())
	}
}

func (t *timeType) ColumnOrder() *format.ColumnOrder {
	return &typeDefinedColumnOrder
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

func (t *timeType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	if t.Unit.Millis != nil {
		return newInt32ColumnIndexer()
	} else {
		return newInt64ColumnIndexer()
	}
}

func (t *timeType) NewDictionary(bufferSize int) Dictionary {
	if t.Unit.Millis != nil {
		return newInt32Dictionary(t, bufferSize)
	} else {
		return newInt64Dictionary(t, bufferSize)
	}
}

func (t *timeType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	if t.Unit.Millis != nil {
		return newInt32ColumnBuffer(columnIndex, bufferSize)
	} else {
		return newInt64ColumnBuffer(columnIndex, bufferSize)
	}
}

func (t *timeType) NewValueDecoder(bufferSize int) ValueDecoder {
	if t.Unit.Millis != nil {
		return newInt32ValueDecoder(bufferSize)
	} else {
		return newInt64ValueDecoder(bufferSize)
	}
}

// Timestamp constructs of leaf node of TIMESTAMP logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
func Timestamp(unit TimeUnit) Node {
	return Leaf(&timestampType{IsAdjustedToUTC: true, Unit: unit.TimeUnit()})
}

type timestampType format.TimestampType

func (t *timestampType) String() string { return (*format.TimestampType)(t).String() }

func (t *timestampType) Kind() Kind { return Int64 }

func (t *timestampType) Length() int { return 64 }

func (t *timestampType) Compare(a, b Value) int { return compareInt64(a.Int64(), b.Int64()) }

func (t *timestampType) ColumnOrder() *format.ColumnOrder { return &typeDefinedColumnOrder }

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

func (t *timestampType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt64ColumnIndexer()
}

func (t *timestampType) NewDictionary(bufferSize int) Dictionary {
	return newInt64Dictionary(t, bufferSize)
}

func (t *timestampType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt64ColumnBuffer(columnIndex, bufferSize)
}

func (t *timestampType) NewValueDecoder(bufferSize int) ValueDecoder {
	return newInt64ValueDecoder(bufferSize)
}

// List constructs a node eof LIST logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
func List(of Node) Node {
	return listNode{Group{"list": Repeated(Group{"element": of})}}
}

type listNode struct{ Group }

func (listNode) Type() Type { return &listType{} }

type listType format.ListType

func (t *listType) String() string { return (*format.ListType)(t).String() }

func (t *listType) Kind() Kind { panic("cannot call Kind on parquet LIST type") }

func (t *listType) Length() int { return 0 }

func (t *listType) Compare(Value, Value) int { panic("cannot compare values on parquet LIST type") }

func (t *listType) ColumnOrder() *format.ColumnOrder { return nil }

func (t *listType) PhyiscalType() *format.Type { return nil }

func (t *listType) LogicalType() *format.LogicalType {
	return &format.LogicalType{List: (*format.ListType)(t)}
}

func (t *listType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.List]
}

func (t *listType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	panic("create create a column indexer from parquet LIST type")
}

func (t *listType) NewDictionary(bufferSize int) Dictionary {
	panic("cannot create dictionary from parquet LIST type")
}

func (t *listType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	panic("cannot create row group column from parquet LIST type")
}

func (t *listType) NewValueDecoder(bufferSize int) ValueDecoder {
	panic("cannot create page reader from parquet LIST type")
}

// Map constructs a node of MAP logical type.
//
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
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

func (t *mapType) String() string { return (*format.MapType)(t).String() }

func (t *mapType) Kind() Kind { panic("cannot call Kind on parquet MAP type") }

func (t *mapType) Length() int { return 0 }

func (t *mapType) Compare(Value, Value) int { panic("cannot compare values on parquet MAP type") }

func (t *mapType) ColumnOrder() *format.ColumnOrder { return nil }

func (t *mapType) PhyiscalType() *format.Type { return nil }

func (t *mapType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Map: (*format.MapType)(t)}
}

func (t *mapType) ConvertedType() *deprecated.ConvertedType {
	return &convertedTypes[deprecated.Map]
}

func (t *mapType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	panic("create create a column indexer from parquet MAP type")
}

func (t *mapType) NewDictionary(bufferSize int) Dictionary {
	panic("cannot create dictionary from parquet MAP type")
}

func (t *mapType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	panic("cannot create row group column from parquet MAP type")
}

func (t *mapType) NewValueDecoder(bufferSize int) ValueDecoder {
	panic("cannot create page reader from parquet MAP type")
}

type nullType format.NullType

func (t *nullType) String() string { return (*format.NullType)(t).String() }

func (t *nullType) Kind() Kind { panic("cannot call Kind on parquet NULL type") }

func (t *nullType) Length() int { return 0 }

func (t *nullType) Compare(Value, Value) int { panic("cannot compare values on parquet NULL type") }

func (t *nullType) ColumnOrder() *format.ColumnOrder { return nil }

func (t *nullType) PhyiscalType() *format.Type { return nil }

func (t *nullType) LogicalType() *format.LogicalType {
	return &format.LogicalType{Unknown: (*format.NullType)(t)}
}

func (t *nullType) ConvertedType() *deprecated.ConvertedType { return nil }

func (t *nullType) NewColumnIndexer(int) ColumnIndexer {
	panic("create create a column indexer from parquet NULL type")
}

func (t *nullType) NewDictionary(int) Dictionary {
	panic("cannot create dictionary from parquet NULL type")
}

func (t *nullType) NewColumnBuffer(int, int) ColumnBuffer {
	panic("cannot create row group column from parquet NULL type")
}

func (t *nullType) NewValueDecoder(int) ValueDecoder {
	panic("cannot create page reader from parquet NULL type")
}

type groupType struct{}

func (groupType) String() string { return "group" }

func (groupType) Kind() Kind {
	panic("cannot call Kind on parquet group")
}

func (groupType) Compare(Value, Value) int {
	panic("cannot compare values on parquet group")
}

func (groupType) NewColumnIndexer(int) ColumnIndexer {
	panic("cannot create column indexer from parquet group")
}

func (groupType) NewDictionary(int) Dictionary {
	panic("cannot create dictionary from parquet group")
}

func (t groupType) NewColumnBuffer(int, int) ColumnBuffer {
	panic("cannot create row group column from parquet group")
}

func (groupType) NewValueDecoder(int) ValueDecoder {
	panic("cannot create value decoder from parquet group")
}

func (groupType) Length() int { return 0 }

func (groupType) ColumnOrder() *format.ColumnOrder { return nil }

func (groupType) PhyiscalType() *format.Type { return nil }

func (groupType) LogicalType() *format.LogicalType { return nil }

func (groupType) ConvertedType() *deprecated.ConvertedType { return nil }

func compareBool(v1, v2 bool) int {
	if v1 != v2 {
		if v2 {
			return -1
		} else {
			return +1
		}
	}
	return 0
}

func compareInt32(v1, v2 int32) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareInt64(v1, v2 int64) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareInt96(v1, v2 deprecated.Int96) int {
	switch {
	case v1.Less(v2):
		return -1
	case v2.Less(v1):
		return +1
	default:
		return 0
	}
}

func compareFloat32(v1, v2 float32) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareFloat64(v1, v2 float64) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareUint32(v1, v2 uint32) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}

func compareUint64(v1, v2 uint64) int {
	switch {
	case v1 < v2:
		return -1
	case v1 > v2:
		return +1
	default:
		return 0
	}
}
