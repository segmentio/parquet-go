package parquet

import "github.com/segmentio/parquet/format"

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

func (index *offsetIndex) PageLocation(i int) (offset, compressedPageSize, firstRowIndex int64) {
	page := &index.PageLocations[i]
	return page.Offset, int64(page.CompressedPageSize), page.FirstRowIndex
}
