//go:build go1.18

package parquet

import (
	"bytes"
	"fmt"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/format"
)

var (
	BooleanType   Type = primitiveType[bool]{class: &boolClass}
	Int32Type     Type = primitiveType[int32]{class: &int32Class}
	Int64Type     Type = primitiveType[int64]{class: &int64Class}
	Int96Type     Type = primitiveType[deprecated.Int96]{class: &int96Class}
	FloatType     Type = primitiveType[float32]{class: &float32Class}
	DoubleType    Type = primitiveType[float64]{class: &float64Class}
	ByteArrayType Type = byteArrayType{}
)

type primitiveType[T primitive] struct{ class *class[T] }

func (t primitiveType[T]) ColumnOrder() *format.ColumnOrder { return &typeDefinedColumnOrder }

func (t primitiveType[T]) PhysicalType() *format.Type { return &physicalTypes[t.class.kind] }

func (t primitiveType[T]) LogicalType() *format.LogicalType { return nil }

func (t primitiveType[T]) ConvertedType() *deprecated.ConvertedType { return nil }

func (t primitiveType[T]) String() string { return t.class.name }

func (t primitiveType[T]) Kind() Kind { return t.class.kind }

func (t primitiveType[T]) Length() int { return int(t.class.bits) }

func (t primitiveType[T]) Compare(a, b Value) int {
	return t.class.compare(t.class.value(a), t.class.value(b))
}

func (t primitiveType[T]) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newColumnIndexer(t.class)
}

func (t primitiveType[T]) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, t.class)
}

func (t primitiveType[T]) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, t.class)
}

func (t primitiveType[T]) NewPage(columnIndex, numValues int, data []byte) Page {
	return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, t.class)
}

type byteArrayType struct{}

func (t byteArrayType) String() string { return "BYTE_ARRAY" }

func (t byteArrayType) Kind() Kind { return ByteArray }

func (t byteArrayType) Length() int { return 0 }

func (t byteArrayType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t byteArrayType) ColumnOrder() *format.ColumnOrder { return &typeDefinedColumnOrder }

func (t byteArrayType) LogicalType() *format.LogicalType { return nil }

func (t byteArrayType) ConvertedType() *deprecated.ConvertedType { return nil }

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

type fixedLenByteArrayType struct{ length int }

func (t *fixedLenByteArrayType) String() string {
	return fmt.Sprintf("FIXED_LEN_BYTE_ARRAY(%d)", t.length)
}

func (t *fixedLenByteArrayType) Kind() Kind { return FixedLenByteArray }

func (t *fixedLenByteArrayType) Length() int { return t.length }

func (t *fixedLenByteArrayType) Compare(a, b Value) int {
	return bytes.Compare(a.ByteArray(), b.ByteArray())
}

func (t fixedLenByteArrayType) ColumnOrder() *format.ColumnOrder { return &typeDefinedColumnOrder }

func (t fixedLenByteArrayType) LogicalType() *format.LogicalType { return nil }

func (t fixedLenByteArrayType) ConvertedType() *deprecated.ConvertedType { return nil }

func (t *fixedLenByteArrayType) PhysicalType() *format.Type { return &physicalTypes[FixedLenByteArray] }

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
func FixedLenByteArrayType(length int) Type { return &fixedLenByteArrayType{length: length} }

func (t *intType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newColumnIndexer(&int64Class)
		} else {
			return newColumnIndexer(&int32Class)
		}
	} else {
		if t.BitWidth == 64 {
			return newColumnIndexer(&uint64Class)
		} else {
			return newColumnIndexer(&uint32Class)
		}
	}
}

func (t *intType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
		} else {
			return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
		}
	} else {
		if t.BitWidth == 64 {
			return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &uint64Class)
		} else {
			return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &uint32Class)
		}
	}
}

func (t *intType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int64Class)
		} else {
			return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int32Class)
		}
	} else {
		if t.BitWidth == 64 {
			return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, &uint64Class)
		} else {
			return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, &uint32Class)
		}
	}
}

func (t *intType) NewPage(columnIndex, numValues int, data []byte) Page {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int64Class)
		} else {
			return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int32Class)
		}
	} else {
		if t.BitWidth == 64 {
			return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, &uint64Class)
		} else {
			return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, &uint32Class)
		}
	}
}

func (t *dateType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newColumnIndexer(&int32Class)
}

func (t *dateType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
}

func (t *dateType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int32Class)
}

func (t *dateType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int32Class)
}

func (t *timeType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	if t.Unit.Millis != nil {
		return newColumnIndexer(&int32Class)
	} else {
		return newColumnIndexer(&int64Class)
	}
}

func (t *timeType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	if t.Unit.Millis != nil {
		return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
	} else {
		return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
	}
}

func (t *timeType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	if t.Unit.Millis != nil {
		return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int32Class)
	} else {
		return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int64Class)
	}
}

func (t *timeType) NewPage(columnIndex, numValues int, data []byte) Page {
	if t.Unit.Millis != nil {
		return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int32Class)
	} else {
		return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int64Class)
	}
}

func (t *timestampType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newColumnIndexer(&int64Class)
}

func (t *timestampType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
}

func (t *timestampType) NewDictionary(columnIndex, numValues int, data []byte) Dictionary {
	return newDictionary(t, makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int64Class)
}

func (t *timestampType) NewPage(columnIndex, numValues int, data []byte) Page {
	return newPage(makeColumnIndex(columnIndex), makeNumValues(numValues), data, &int64Class)
}
