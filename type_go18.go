//go:build go1.18

package parquet

import (
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
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

func (t primitiveType[T]) NewDictionary(columnIndex, bufferSize int) Dictionary {
	return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, t.class)
}

func (t primitiveType[T]) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, t.class)
}

func (t primitiveType[T]) NewColumnReader(columnIndex, bufferSize int) ColumnReader {
	return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, t.class)
}

func (t primitiveType[T]) NewPageDecoder(columnIndex, numValues int, decoder encoding.Decoder) PageValues {
	return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, t.class)
}

func (t primitiveType[T]) ReadDictionary(columnIndex, numValues int, decoder encoding.Decoder) (Dictionary, error) {
	return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, t.class)
}

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

func (t *intType) NewDictionary(columnIndex, bufferSize int) Dictionary {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
		} else {
			return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
		}
	} else {
		if t.BitWidth == 64 {
			return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, &uint64Class)
		} else {
			return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, &uint32Class)
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

func (t *intType) NewColumnReader(columnIndex, bufferSize int) ColumnReader {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
		} else {
			return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
		}
	} else {
		if t.BitWidth == 64 {
			return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, &uint64Class)
		} else {
			return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, &uint32Class)
		}
	}
}

func (t *intType) NewPageDecoder(columnIndex, numValues int, decoder encoding.Decoder) PageValues {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, &int64Class)
		} else {
			return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, &int32Class)
		}
	} else {
		if t.BitWidth == 64 {
			return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, &uint64Class)
		} else {
			return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, &uint32Class)
		}
	}
}

func (t *intType) ReadDictionary(columnIndex, numValues int, decoder encoding.Decoder) (Dictionary, error) {
	if t.IsSigned {
		if t.BitWidth == 64 {
			return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, &int64Class)
		} else {
			return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, &int32Class)
		}
	} else {
		if t.BitWidth == 64 {
			return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, &uint64Class)
		} else {
			return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, &uint32Class)
		}
	}
}

func (t *dateType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newColumnIndexer(&int32Class)
}

func (t *dateType) NewDictionary(columnIndex, bufferSize int) Dictionary {
	return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
}

func (t *dateType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
}

func (t *dateType) NewColumnReader(columnIndex, bufferSize int) ColumnReader {
	return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
}

func (t *dateType) NewPageDecoder(columnIndex, numValues int, decoder encoding.Decoder) PageValues {
	return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, &int32Class)
}

func (t *dateType) ReadDictionary(columnIndex, numValues int, decoder encoding.Decoder) (Dictionary, error) {
	return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, &int32Class)
}

func (t *timeType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	if t.Unit.Millis != nil {
		return newColumnIndexer(&int32Class)
	} else {
		return newColumnIndexer(&int64Class)
	}
}

func (t *timeType) NewDictionary(columnIndex, bufferSize int) Dictionary {
	if t.Unit.Millis != nil {
		return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
	} else {
		return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
	}
}

func (t *timeType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	if t.Unit.Millis != nil {
		return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
	} else {
		return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
	}
}

func (t *timeType) NewColumnReader(columnIndex, bufferSize int) ColumnReader {
	if t.Unit.Millis != nil {
		return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, &int32Class)
	} else {
		return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
	}
}

func (t *timeType) NewPageDecoder(columnIndex, numValues int, decoder encoding.Decoder) PageValues {
	if t.Unit.Millis != nil {
		return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, &int32Class)
	} else {
		return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, &int64Class)
	}
}

func (t *timeType) ReadDictionary(columnIndex, numValues int, decoder encoding.Decoder) (Dictionary, error) {
	if t.Unit.Millis != nil {
		return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, &int32Class)
	} else {
		return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, &int64Class)
	}
}

func (t *timestampType) NewColumnIndexer(sizeLimit int) ColumnIndexer {
	return newColumnIndexer(&int64Class)
}

func (t *timestampType) NewDictionary(columnIndex, bufferSize int) Dictionary {
	return newDictionary(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
}

func (t *timestampType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newColumnBuffer(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
}

func (t *timestampType) NewColumnReader(columnIndex, bufferSize int) ColumnReader {
	return newColumnReader(t, makeColumnIndex(columnIndex), bufferSize, &int64Class)
}

func (t *timestampType) NewPageDecoder(columnIndex, numValues int, decoder encoding.Decoder) PageValues {
	return newPageDecoder(t, makeColumnIndex(columnIndex), numValues, decoder, &int64Class)
}

func (t *timestampType) ReadDictionary(columnIndex, numValues int, decoder encoding.Decoder) (Dictionary, error) {
	return readDictionary(t, makeColumnIndex(columnIndex), numValues, decoder, &int64Class)
}
