package parquet

import (
	"bytes"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
)

type ColumnIndex interface {
	// NumPages returns the number of paged in the column index.
	NumPages() int

	// Returns the number of null values in the page at the given index.
	NullCount(int) int64

	// Tells whether the page at the given index contains null values only.
	NullPage(int) bool

	// PageIndex return min/max bounds for the page at the given index in the
	// column.
	MinValue(int) []byte
	MaxValue(int) []byte

	// IsAscending returns true if the column index min/max values are sorted
	// in ascending order (based on the ordering rules of the column's logical
	// type).
	IsAscending() bool

	// IsDescending returns true if the column index min/max values are sorted
	// in descending order (based on the ordering rules of the column's logical
	// type).
	IsDescending() bool
}

type columnIndex format.ColumnIndex

func (i *columnIndex) NumPages() int         { return len(i.NullPages) }
func (i *columnIndex) NullCount(j int) int64 { return i.NullCounts[j] }
func (i *columnIndex) NullPage(j int) bool   { return i.NullPages[j] }
func (i *columnIndex) MinValue(j int) []byte { return i.MinValues[j] }
func (i *columnIndex) MaxValue(j int) []byte { return i.MaxValues[j] }
func (i *columnIndex) IsAscending() bool     { return i.BoundaryOrder == format.Ascending }
func (i *columnIndex) IsDescending() bool    { return i.BoundaryOrder == format.Descending }

type byteArrayPageIndex struct{ page *byteArrayPage }

func (i byteArrayPageIndex) NumPages() int       { return 1 }
func (i byteArrayPageIndex) NullCount(int) int64 { return 0 }
func (i byteArrayPageIndex) NullPage(int) bool   { return false }
func (i byteArrayPageIndex) MinValue(int) []byte { return copyBytes(i.page.min()) }
func (i byteArrayPageIndex) MaxValue(int) []byte { return copyBytes(i.page.max()) }
func (i byteArrayPageIndex) IsAscending() bool   { return bytes.Compare(i.page.bounds()) < 0 }
func (i byteArrayPageIndex) IsDescending() bool  { return bytes.Compare(i.page.bounds()) > 0 }

type fixedLenByteArrayPageIndex struct{ page *fixedLenByteArrayPage }

func (i fixedLenByteArrayPageIndex) NumPages() int       { return 1 }
func (i fixedLenByteArrayPageIndex) NullCount(int) int64 { return 0 }
func (i fixedLenByteArrayPageIndex) NullPage(int) bool   { return false }
func (i fixedLenByteArrayPageIndex) MinValue(int) []byte { return copyBytes(i.page.min()) }
func (i fixedLenByteArrayPageIndex) MaxValue(int) []byte { return copyBytes(i.page.max()) }
func (i fixedLenByteArrayPageIndex) IsAscending() bool   { return bytes.Compare(i.page.bounds()) < 0 }
func (i fixedLenByteArrayPageIndex) IsDescending() bool  { return bytes.Compare(i.page.bounds()) > 0 }

// The ColumnIndexer interface is implemented by types that support generating
// parquet column indexes.
//
// The package does not export any types that implement this interface, programs
// must call NewColumnIndexer on a Type instance to construct column indexers.
type ColumnIndexer interface {
	// Resets the column indexer state.
	Reset()

	// Add a page to the column indexer.
	IndexPage(numValues, numNulls int64, min, max Value)

	// Generates a format.ColumnIndex value from the current state of the
	// column indexer.
	//
	// The returned value may reference internal buffers, in which case the
	// values remain valid until the next call to IndexPage or Reset on the
	// column indexer.
	ColumnIndex() format.ColumnIndex
}

type baseColumnIndexer struct {
	nullPages  []bool
	nullCounts []int64
}

func (i *baseColumnIndexer) reset() {
	i.nullPages = i.nullPages[:0]
	i.nullCounts = i.nullCounts[:0]
}

func (i *baseColumnIndexer) observe(numValues, numNulls int64) {
	i.nullPages = append(i.nullPages, numValues == numNulls)
	i.nullCounts = append(i.nullCounts, numNulls)
}

func (i *baseColumnIndexer) columnIndex(minValues, maxValues [][]byte, minOrder, maxOrder int) format.ColumnIndex {
	return format.ColumnIndex{
		NullPages:     i.nullPages,
		NullCounts:    i.nullCounts,
		MinValues:     minValues,
		MaxValues:     maxValues,
		BoundaryOrder: boundaryOrderOf(minOrder, maxOrder),
	}
}

type byteArrayColumnIndexer struct {
	baseColumnIndexer
	sizeLimit int
	minValues encoding.ByteArrayList
	maxValues encoding.ByteArrayList
}

func newByteArrayColumnIndexer(sizeLimit int) *byteArrayColumnIndexer {
	return &byteArrayColumnIndexer{sizeLimit: sizeLimit}
}

func (i *byteArrayColumnIndexer) Reset() {
	i.reset()
	i.minValues.Reset()
	i.maxValues.Reset()
}

func (i *byteArrayColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues.Push(min.ByteArray())
	i.maxValues.Push(max.ByteArray())
}

func (i *byteArrayColumnIndexer) ColumnIndex() format.ColumnIndex {
	minValues := i.minValues.Split()
	maxValues := i.maxValues.Split()
	if i.sizeLimit > 0 {
		truncateLargeMinByteArrayValues(minValues, i.sizeLimit)
		truncateLargeMaxByteArrayValues(maxValues, i.sizeLimit)
	}
	return i.columnIndex(
		minValues,
		maxValues,
		bits.OrderOfBytes(minValues),
		bits.OrderOfBytes(maxValues),
	)
}

func truncateLargeMinByteArrayValues(values [][]byte, sizeLimit int) {
	for i, v := range values {
		if len(v) > sizeLimit {
			values[i] = v[:sizeLimit]
		}
	}
}

func truncateLargeMaxByteArrayValues(values [][]byte, sizeLimit int) {
	if !hasLongerValuesThanSizeLimit(values, sizeLimit) {
		return
	}

	// Rather than allocating a new byte slice for each value that exceeds the
	// limit, a single buffer is allocated to hold all the values. This makes
	// the GC cost of this function a constant rather than being linear to the
	// number of values in the input slice.
	b := make([]byte, len(values)*sizeLimit)

	for i, v := range values {
		if len(v) > sizeLimit {
			// If v is the max value we cannot truncate it since there are no
			// shorter byte sequence with a greater value. This condition should
			// never occur unless the input was especially constructed to trigger
			// it.
			if !isMaxByteArrayValue(v) {
				j := (i + 0) * sizeLimit
				k := (i + 1) * sizeLimit
				x := b[j:k:k]
				copy(x, v)
				values[i] = nextByteArrayValue(x)
			}
		}
	}
}

func hasLongerValuesThanSizeLimit(values [][]byte, sizeLimit int) bool {
	for _, v := range values {
		if len(v) > sizeLimit {
			return true
		}
	}
	return false
}

func isMaxByteArrayValue(value []byte) bool {
	for i := range value {
		if value[i] != 0xFF {
			return false
		}
	}
	return true
}

func nextByteArrayValue(value []byte) []byte {
	for i := len(value) - 1; i > 0; i-- {
		if value[i]++; value[i] != 0 {
			break
		}
		// Overflow: increment the next byte
	}
	return value
}

type fixedLenByteArrayColumnIndexer struct {
	baseColumnIndexer
	size      int
	sizeLimit int
	minValues []byte
	maxValues []byte
}

func newFixedLenByteArrayColumnIndexer(size, sizeLimit int) *fixedLenByteArrayColumnIndexer {
	return &fixedLenByteArrayColumnIndexer{
		size:      size,
		sizeLimit: sizeLimit,
	}
}

func (i *fixedLenByteArrayColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
}

func (i *fixedLenByteArrayColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	i.minValues = append(i.minValues, min.ByteArray()...)
	i.maxValues = append(i.maxValues, max.ByteArray()...)
}

func (i *fixedLenByteArrayColumnIndexer) ColumnIndex() format.ColumnIndex {
	minValues := splitFixedLenByteArrayList(i.size, i.minValues)
	maxValues := splitFixedLenByteArrayList(i.size, i.maxValues)
	if i.sizeLimit > 0 && i.sizeLimit < i.size {
		truncateLargeMinByteArrayValues(minValues, i.sizeLimit)
		truncateLargeMaxByteArrayValues(maxValues, i.sizeLimit)
	}
	return i.columnIndex(
		minValues,
		maxValues,
		bits.OrderOfBytes(minValues),
		bits.OrderOfBytes(maxValues),
	)
}

func splitFixedLenByteArrayList(size int, data []byte) [][]byte {
	data = copyBytes(data)
	values := make([][]byte, len(data)/size)
	for i := range values {
		j := (i + 0) * size
		k := (i + 1) * size
		values[i] = data[j:k:k]
	}
	return values
}

func boundaryOrderOf(minOrder, maxOrder int) format.BoundaryOrder {
	if minOrder == maxOrder {
		switch {
		case minOrder > 0:
			return format.Ascending
		case minOrder < 0:
			return format.Descending
		}
	}
	return format.Unordered
}
