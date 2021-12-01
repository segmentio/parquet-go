package parquet

import "github.com/segmentio/parquet/format"

// OffsetIndex is the data structure representing offset indexes.
type OffsetIndex format.OffsetIndex

// NumPages returns the number of pages in the offset index.
func (index *OffsetIndex) NumPages() int {
	return len(index.PageLocations)
}

// PageLocation returns the location of the page at index i.
//
// The offset and compressed page size are expressed in bytes, the first row
// index is relative to the beginning of the row group that the page belongs to.
func (index *OffsetIndex) PageLocation(i int) (offset int64, compressedPageSize int32, firstRowIndex int) {
	page := &index.PageLocations[i]
	return page.Offset, page.CompressedPageSize, int(page.FirstRowIndex)
}
