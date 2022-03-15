package parquet

import (
	"github.com/segmentio/parquet-go/format"
)

type OffsetIndex interface {
	// NumPages returns the number of pages in the offset index.
	NumPages() int

	// Offset returns the offset starting from the beginning of the file for the
	// page at the given index.
	Offset(int) int64

	// CompressedPageSize returns the size of the page at the given index
	// (in bytes).
	CompressedPageSize(int) int64

	// FirstRowIndex returns the the first row in the page at the given index.
	//
	// The returned row index is based on the row group that the page belongs
	// to, the first row has index zero.
	FirstRowIndex(int) int64
}

// OffsetIndex is the data structure representing offset indexes.
type offsetIndex format.OffsetIndex

func (index *offsetIndex) NumPages() int {
	return len(index.PageLocations)
}

func (index *offsetIndex) Offset(i int) int64 {
	return index.PageLocations[i].Offset
}

func (index *offsetIndex) CompressedPageSize(i int) int64 {
	return int64(index.PageLocations[i].CompressedPageSize)
}

func (index *offsetIndex) FirstRowIndex(i int) int64 {
	return index.PageLocations[i].FirstRowIndex
}

func (index byteArrayPageIndex) Offset(int) int64             { return 0 }
func (index byteArrayPageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index byteArrayPageIndex) FirstRowIndex(int) int64      { return 0 }

func (index fixedLenByteArrayPageIndex) Offset(int) int64             { return 0 }
func (index fixedLenByteArrayPageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index fixedLenByteArrayPageIndex) FirstRowIndex(int) int64      { return 0 }
