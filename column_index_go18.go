//go:build go1.18

package parquet

import (
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/cast"
)

type pageIndex[T primitive] struct{ page *page[T] }

// ColumnIndex

func (i pageIndex[T]) NumPages() int       { return 1 }
func (i pageIndex[T]) NullCount(int) int64 { return 0 }
func (i pageIndex[T]) NullPage(int) bool   { return false }
func (i pageIndex[T]) MinValue(int) []byte { return i.page.class.plain(i.page.min()) }
func (i pageIndex[T]) MaxValue(int) []byte { return i.page.class.plain(i.page.max()) }
func (i pageIndex[T]) IsAscending() bool   { return i.page.class.compare(i.page.bounds()) < 0 }
func (i pageIndex[T]) IsDescending() bool  { return i.page.class.compare(i.page.bounds()) > 0 }

// OffsetIndex

func (i pageIndex[T]) Offset(int) int64             { return 0 }
func (i pageIndex[T]) CompressedPageSize(int) int64 { return i.page.Size() }
func (i pageIndex[T]) FirstRowIndex(int) int64      { return 0 }

type columnIndexer[T primitive] struct {
	class      *class[T]
	nullPages  []bool
	nullCounts []int64
	minValues  []T
	maxValues  []T
}

func (i *columnIndexer[T]) Reset() {
	i.nullPages = i.nullPages[:0]
	i.nullCounts = i.nullCounts[:0]
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
}

func (i *columnIndexer[T]) IndexPage(numValues, numNulls int64, min, max Value) {
	i.nullPages = append(i.nullPages, numValues == numNulls)
	i.nullCounts = append(i.nullCounts, numNulls)
	i.minValues = append(i.minValues, i.class.valueOf(min))
	i.maxValues = append(i.maxValues, i.class.valueOf(max))
}

func (i *columnIndexer[T]) ColumnIndex() format.ColumnIndex {
	minValues := splitFixedLenByteArrayList(sizeof[T](), cast.SliceToBytes(i.minValues))
	maxValues := splitFixedLenByteArrayList(sizeof[T](), cast.SliceToBytes(i.maxValues))
	minOrder := i.class.order(i.minValues)
	maxOrder := i.class.order(i.maxValues)
	return format.ColumnIndex{
		NullPages:     i.nullPages,
		NullCounts:    i.nullCounts,
		MinValues:     minValues,
		MaxValues:     maxValues,
		BoundaryOrder: boundaryOrderOf(minOrder, maxOrder),
	}
}

func newBooleanColumnIndexer() *columnIndexer[bool] {
	return &columnIndexer[bool]{class: &boolClass}
}

func newInt32ColumnIndexer() *columnIndexer[int32] {
	return &columnIndexer[int32]{class: &int32Class}
}

func newInt64ColumnIndexer() *columnIndexer[int64] {
	return &columnIndexer[int64]{class: &int64Class}
}

func newInt96ColumnIndexer() *columnIndexer[deprecated.Int96] {
	return &columnIndexer[deprecated.Int96]{class: &int96Class}
}

func newFloatColumnIndexer() *columnIndexer[float32] {
	return &columnIndexer[float32]{class: &float32Class}
}

func newDoubleColumnIndexer() *columnIndexer[float64] {
	return &columnIndexer[float64]{class: &float64Class}
}

func newUint32ColumnIndexer() *columnIndexer[uint32] {
	return &columnIndexer[uint32]{class: &uint32Class}
}

func newUint64ColumnIndexer() *columnIndexer[uint64] {
	return &columnIndexer[uint64]{class: &uint64Class}
}
