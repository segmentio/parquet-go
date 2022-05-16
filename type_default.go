//go:build !go1.18

package parquet

import (
	"bytes"
	"fmt"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/format"
)

var (
	BooleanType   Type = booleanType{}
	Int32Type     Type = int32Type{}
	Int64Type     Type = int64Type{}
	Int96Type     Type = int96Type{}
	FloatType     Type = floatType{}
	DoubleType    Type = doubleType{}
	ByteArrayType Type = byteArrayType{}
)

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

func (t booleanType) PhysicalType() *format.Type {
	return &physicalTypes[Boolean]
}

func (t booleanType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newBooleanColumnIndexer()
}

func (t booleanType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newBooleanColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t booleanType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newBooleanDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t booleanType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newBooleanPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

type int32Type struct{ primitiveType }

func (t int32Type) String() string { return "INT32" }

func (t int32Type) Kind() Kind { return Int32 }

func (t int32Type) Length() int { return 32 }

func (t int32Type) Compare(a, b Value) int {
	return compareInt32(a.Int32(), b.Int32())
}

func (t int32Type) PhysicalType() *format.Type {
	return &physicalTypes[Int32]
}

func (t int32Type) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt32ColumnIndexer()
}

func (t int32Type) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt32ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t int32Type) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newInt32Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t int32Type) NewPage(columnIndex, numValues int, data []byte) Page {
	return newInt32Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

type int64Type struct{ primitiveType }

func (t int64Type) String() string { return "INT64" }

func (t int64Type) Kind() Kind { return Int64 }

func (t int64Type) Length() int { return 64 }

func (t int64Type) Compare(a, b Value) int {
	return compareInt64(a.Int64(), b.Int64())
}

func (t int64Type) PhysicalType() *format.Type {
	return &physicalTypes[Int64]
}

func (t int64Type) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt64ColumnIndexer()
}

func (t int64Type) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt64ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t int64Type) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newInt64Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t int64Type) NewPage(columnIndex, numValues int, data []byte) Page {
	return newInt64Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

type int96Type struct{ primitiveType }

func (t int96Type) String() string { return "INT96" }

func (t int96Type) Kind() Kind { return Int96 }

func (t int96Type) Length() int { return 96 }

func (t int96Type) Compare(a, b Value) int {
	return compareInt96(a.Int96(), b.Int96())
}

func (t int96Type) PhysicalType() *format.Type {
	return &physicalTypes[Int96]
}

func (t int96Type) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt96ColumnIndexer()
}

func (t int96Type) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt96ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t int96Type) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newInt96Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t int96Type) NewPage(columnIndex, numValues int, data []byte) Page {
	return newInt96Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

type floatType struct{ primitiveType }

func (t floatType) String() string { return "FLOAT" }

func (t floatType) Kind() Kind { return Float }

func (t floatType) Length() int { return 32 }

func (t floatType) Compare(a, b Value) int {
	return compareFloat32(a.Float(), b.Float())
}

func (t floatType) PhysicalType() *format.Type {
	return &physicalTypes[Float]
}

func (t floatType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newFloatColumnIndexer()
}

func (t floatType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newFloatColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t floatType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newFloatDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t floatType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newFloatPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

type doubleType struct{ primitiveType }

func (t doubleType) String() string { return "DOUBLE" }

func (t doubleType) Kind() Kind { return Double }

func (t doubleType) Length() int { return 64 }

func (t doubleType) Compare(a, b Value) int {
	return compareFloat64(a.Double(), b.Double())
}

func (t doubleType) PhysicalType() *format.Type { return &physicalTypes[Double] }

func (t doubleType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newDoubleColumnIndexer()
}

func (t doubleType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newDoubleColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t doubleType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newDoubleDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t doubleType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newDoublePage(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

type byteArrayType struct{ primitiveType }

func (t byteArrayType) String() string { return "BYTE_ARRAY" }

func (t byteArrayType) Kind() Kind { return ByteArray }

func (t byteArrayType) Length() int { return 0 }

func (t byteArrayType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t byteArrayType) PhysicalType() *format.Type {
	return &physicalTypes[ByteArray]
}

func (t byteArrayType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newByteArrayColumnIndexer(sizeLimit)
}

func (t byteArrayType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newByteArrayColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t byteArrayType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newByteArrayDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t byteArrayType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newByteArrayPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
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

func (t *fixedLenByteArrayType) PhysicalType() *format.Type {
	return &physicalTypes[FixedLenByteArray]
}

func (t *fixedLenByteArrayType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newFixedLenByteArrayColumnIndexer(t.length, sizeLimit)
}

func (t *fixedLenByteArrayType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newFixedLenByteArrayColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t *fixedLenByteArrayType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newFixedLenByteArrayDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t fixedLenByteArrayType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newFixedLenByteArrayPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, t.Length())
}

// FixedLenByteArrayType constructs a type for fixed-length values of the given
// size (in bytes).
func FixedLenByteArrayType(length int) Type {
	return &fixedLenByteArrayType{length: length}
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

func (t *intType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newInt64ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
		} else {
			return newInt32ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
		}
	} else {
		if t.BitWidth == 64 {
			return newUint64ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
		} else {
			return newUint32ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
		}
	}
}

func (t *intType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newInt64Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
		} else {
			return newInt32Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
		}
	} else {
		if t.BitWidth == 64 {
			return newUint64Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
		} else {
			return newUint32Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
		}
	}
}

func (t *intType) NewPage(columnIndex, numValues int, data []byte) Page {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newInt64Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
		} else {
			return newInt32Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
		}
	} else {
		if t.BitWidth == 64 {
			return newUint64Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
		} else {
			return newUint32Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
		}
	}
}

func (t *dateType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt32ColumnIndexer()
}

func (t *dateType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt32ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t *dateType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newInt32Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t *dateType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newInt32Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t *timeType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	if t.Unit.Millis != nil {
		return newInt32ColumnIndexer()
	} else {
		return newInt64ColumnIndexer()
	}
}

func (t *timeType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	if t.Unit.Millis != nil {
		return newInt32ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
	} else {
		return newInt64ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
	}
}

func (t *timeType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	if t.Unit.Millis != nil {
		return newInt32Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
	} else {
		return newInt64Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
	}
}

func (t *timeType) NewPage(columnIndex, numValues int, data []byte) Page {
	if t.Unit.Millis != nil {
		return newInt32Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
	} else {
		return newInt64Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
	}
}

func (t *timestampType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newInt64ColumnIndexer()
}

func (t *timestampType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newInt64ColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize)
}

func (t *timestampType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newInt64Dictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}

func (t *timestampType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newInt64Page(makeColumnIndex(columnIndex), makeNumValues(numValues), data)
}
