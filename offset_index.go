package parquet

import (
	"github.com/segmentio/parquet/format"
)

type OffsetIndex interface {
	// NumPages returns the number of pages in the offset index.
	NumPages() int

	// PageLocation returns the location of the page at the given index.
	//
	// The offset and compressed page size are expressed in bytes, the first row
	// index is relative to the beginning of the row group.
	PageLocation(int) (offset, compressedPageSize, firstRowIndex int64)
}

// OffsetIndex is the data structure representing offset indexes.
type offsetIndex format.OffsetIndex

func (index *offsetIndex) NumPages() int {
	return len(index.PageLocations)
}

func (index *offsetIndex) PageLocation(i int) (int64, int64, int64) {
	page := &index.PageLocations[i]
	return page.Offset, int64(page.CompressedPageSize), page.FirstRowIndex
}

func (index booleanPageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index int32PageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index int64PageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index int96PageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index floatPageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index doublePageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index byteArrayPageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index fixedLenByteArrayPageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index uint32PageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}

func (index uint64PageIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.page.Size(), 0
}
