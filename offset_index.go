package parquet

import (
	"github.com/segmentio/parquet-go/format"
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

type booleanOffsetIndex struct{ *booleanColumnBuffer }

func (index booleanOffsetIndex) NumPages() int                          { return 1 }
func (index booleanOffsetIndex) PageLocation(int) (int64, int64, int64) { return 0, index.Size(), 0 }

type int32OffsetIndex struct{ *int32ColumnBuffer }

func (index int32OffsetIndex) NumPages() int                          { return 1 }
func (index int32OffsetIndex) PageLocation(int) (int64, int64, int64) { return 0, index.Size(), 0 }

type int64OffsetIndex struct{ *int64ColumnBuffer }

func (index int64OffsetIndex) NumPages() int                          { return 1 }
func (index int64OffsetIndex) PageLocation(int) (int64, int64, int64) { return 0, index.Size(), 0 }

type int96OffsetIndex struct{ *int96ColumnBuffer }

func (index int96OffsetIndex) NumPages() int                          { return 1 }
func (index int96OffsetIndex) PageLocation(int) (int64, int64, int64) { return 0, index.Size(), 0 }

type floatOffsetIndex struct{ *floatColumnBuffer }

func (index floatOffsetIndex) NumPages() int                          { return 1 }
func (index floatOffsetIndex) PageLocation(int) (int64, int64, int64) { return 0, index.Size(), 0 }

type doubleOffsetIndex struct{ *doubleColumnBuffer }

func (index doubleOffsetIndex) NumPages() int                          { return 1 }
func (index doubleOffsetIndex) PageLocation(int) (int64, int64, int64) { return 0, index.Size(), 0 }

type byteArrayOffsetIndex struct{ *byteArrayColumnBuffer }

func (index byteArrayOffsetIndex) NumPages() int                          { return 1 }
func (index byteArrayOffsetIndex) PageLocation(int) (int64, int64, int64) { return 0, index.Size(), 0 }

type fixedLenByteArrayOffsetIndex struct{ *fixedLenByteArrayColumnBuffer }

func (index fixedLenByteArrayOffsetIndex) NumPages() int { return 1 }
func (index fixedLenByteArrayOffsetIndex) PageLocation(int) (int64, int64, int64) {
	return 0, index.Size(), 0
}
