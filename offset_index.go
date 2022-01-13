package parquet

import (
	"github.com/segmentio/parquet/format"
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

func (index booleanPageIndex) Offset(int) int64             { return 0 }
func (index booleanPageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index booleanPageIndex) FirstRowIndex(int) int64      { return 0 }

func (index int32PageIndex) Offset(int) int64             { return 0 }
func (index int32PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index int32PageIndex) FirstRowIndex(int) int64      { return 0 }

func (index int64PageIndex) Offset(int) int64             { return 0 }
func (index int64PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index int64PageIndex) FirstRowIndex(int) int64      { return 0 }

func (index int96PageIndex) Offset(int) int64             { return 0 }
func (index int96PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index int96PageIndex) FirstRowIndex(int) int64      { return 0 }

func (index floatPageIndex) Offset(int) int64             { return 0 }
func (index floatPageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index floatPageIndex) FirstRowIndex(int) int64      { return 0 }

func (index doublePageIndex) Offset(int) int64             { return 0 }
func (index doublePageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index doublePageIndex) FirstRowIndex(int) int64      { return 0 }

func (index byteArrayPageIndex) Offset(int) int64             { return 0 }
func (index byteArrayPageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index byteArrayPageIndex) FirstRowIndex(int) int64      { return 0 }

func (index fixedLenByteArrayPageIndex) Offset(int) int64             { return 0 }
func (index fixedLenByteArrayPageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index fixedLenByteArrayPageIndex) FirstRowIndex(int) int64      { return 0 }

func (index uint32PageIndex) Offset(int) int64             { return 0 }
func (index uint32PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index uint32PageIndex) FirstRowIndex(int) int64      { return 0 }

func (index uint64PageIndex) Offset(int) int64             { return 0 }
func (index uint64PageIndex) CompressedPageSize(int) int64 { return index.page.Size() }
func (index uint64PageIndex) FirstRowIndex(int) int64      { return 0 }
