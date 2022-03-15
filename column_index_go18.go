//go:build go1.18

package parquet

type pageIndex[T primitive] struct {
	page *page[T]
}

// ColumnIndex

func (index pageIndex[T]) NumPages() int {
	return 1
}

func (index pageIndex[T]) NullCount(int) int64 {
	return 0
}

func (index pageIndex[T]) NullPage(int) bool {
	return false
}

func (index pageIndex[T]) MinValue(int) []byte {
	return index.page.class.plain(index.page.min())
}

func (index pageIndex[T]) MaxValue(int) []byte {
	return index.page.class.plain(index.page.max())
}

func (index pageIndex[T]) IsAscending() bool {
	return index.page.class.compare(index.page.bounds()) < 0
}

func (index pageIndex[T]) IsDescending() bool {
	return index.page.class.compare(index.page.bounds()) > 0
}

// OffsetIndex

func (index pageIndex[T]) Offset(int) int64 {
	return 0
}

func (index pageIndex[T]) CompressedPageSize(int) int64 {
	return index.page.Size()
}

func (index pageIndex[T]) FirstRowIndex(int) int64 {
	return 0
}
