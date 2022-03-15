//go:build go1.18

package parquet

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/internal/cast"
)

type columnBuffer[T primitive] struct {
	page[T]
	typ Type
}

func newBooleanColumnBuffer(typ Type, columnIndex int16, bufferSize int) *columnBuffer[bool] {
	return newColumnBuffer(typ, columnIndex, bufferSize, &boolClass)
}

func newInt32ColumnBuffer(typ Type, columnIndex int16, bufferSize int) *columnBuffer[int32] {
	return newColumnBuffer(typ, columnIndex, bufferSize, &int32Class)
}

func newInt64ColumnBuffer(typ Type, columnIndex int16, bufferSize int) *columnBuffer[int64] {
	return newColumnBuffer(typ, columnIndex, bufferSize, &int64Class)
}

func newInt96ColumnBuffer(typ Type, columnIndex int16, bufferSize int) *columnBuffer[deprecated.Int96] {
	return newColumnBuffer(typ, columnIndex, bufferSize, &int96Class)
}

func newFloatColumnBuffer(typ Type, columnIndex int16, bufferSize int) *columnBuffer[float32] {
	return newColumnBuffer(typ, columnIndex, bufferSize, &float32Class)
}

func newDoubleColumnBuffer(typ Type, columnIndex int16, bufferSize int) *columnBuffer[float64] {
	return newColumnBuffer(typ, columnIndex, bufferSize, &float64Class)
}

func newUint32ColumnBuffer(typ Type, columnIndex int16, bufferSize int) *columnBuffer[uint32] {
	return newColumnBuffer(typ, columnIndex, bufferSize, &uint32Class)
}

func newUint64ColumnBuffer(typ Type, columnIndex int16, bufferSize int) *columnBuffer[uint64] {
	return newColumnBuffer(typ, columnIndex, bufferSize, &uint64Class)
}

func newColumnBuffer[T primitive](typ Type, columnIndex int16, bufferSize int, class *class[T]) *columnBuffer[T] {
	return &columnBuffer[T]{
		page: page[T]{
			class:       class,
			values:      make([]T, 0, bufferSize/sizeof[T]()),
			columnIndex: ^columnIndex,
		},
		typ: typ,
	}
}

func (col *columnBuffer[T]) Clone() ColumnBuffer {
	return &columnBuffer[T]{
		page: page[T]{
			class:       col.page.class,
			values:      append([]T{}, col.values...),
			columnIndex: col.columnIndex,
		},
		typ: col.typ,
	}
}

func (col *columnBuffer[T]) Type() Type { return col.typ }

func (col *columnBuffer[T]) ColumnIndex() ColumnIndex { return pageIndex[T]{&col.page} }

func (col *columnBuffer[T]) OffsetIndex() OffsetIndex { return pageIndex[T]{&col.page} }

func (col *columnBuffer[T]) BloomFilter() BloomFilter { return nil }

func (col *columnBuffer[T]) Dictionary() Dictionary { return nil }

func (col *columnBuffer[T]) Pages() Pages { return onePage(col.Page()) }

func (col *columnBuffer[T]) Page() BufferedPage { return &col.page }

func (col *columnBuffer[T]) Reset() { col.values = col.values[:0] }

func (col *columnBuffer[T]) Cap() int { return cap(col.values) }

func (col *columnBuffer[T]) Len() int { return len(col.values) }

func (col *columnBuffer[T]) Less(i, j int) bool {
	return col.class.less(col.values[i], col.values[j])
}

func (col *columnBuffer[T]) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *columnBuffer[T]) Write(b []byte) (int, error) {
	if (len(b) % sizeof[T]()) != 0 {
		return 0, fmt.Errorf("cannot write %s values from input of size %d", col.class.name, len(b))
	}
	return col.WriteRequired(cast.BytesToSlice[T](b))
}

func (col *columnBuffer[T]) WriteRequired(values []T) (int, error) {
	col.values = append(col.values, values...)
	return len(values), nil
}

func (col *columnBuffer[T]) WriteValues(values []Value) (int, error) {
	valueOf := col.class.valueOf
	for _, v := range values {
		col.values = append(col.values, valueOf(v))
	}
	return len(values), nil
}

func (col *columnBuffer[T]) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(int64(len(row)))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(int64(len(row)))
	}
	col.values = append(col.values, col.class.valueOf(row[0]))
	return nil
}

func (col *columnBuffer[T]) ReadRowAt(row Row, index int64) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, int64(len(col.values)))
	case index >= int64(len(col.values)):
		return row, io.EOF
	default:
		v := col.class.makeValue(col.values[index])
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}
