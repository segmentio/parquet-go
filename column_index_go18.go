//go:build go1.18

package parquet

import (
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

func newColumnIndexer[T primitive](class *class[T]) *columnIndexer[T] {
	return &columnIndexer[T]{class: class}
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
	i.minValues = append(i.minValues, i.class.value(min))
	i.maxValues = append(i.maxValues, i.class.value(max))
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
