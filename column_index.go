package parquet

import (
	"bytes"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/plain"
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

func (index *columnIndex) NumPages() int         { return len(index.NullPages) }
func (index *columnIndex) NullCount(i int) int64 { return index.NullCounts[i] }
func (index *columnIndex) NullPage(i int) bool   { return index.NullPages[i] }
func (index *columnIndex) MinValue(i int) []byte { return index.MinValues[i] }
func (index *columnIndex) MaxValue(i int) []byte { return index.MaxValues[i] }
func (index *columnIndex) IsAscending() bool     { return index.BoundaryOrder == format.Ascending }
func (index *columnIndex) IsDescending() bool    { return index.BoundaryOrder == format.Descending }

type booleanPageIndex struct{ page *booleanPage }

func (index booleanPageIndex) NumPages() int       { return 1 }
func (index booleanPageIndex) NullCount(int) int64 { return 0 }
func (index booleanPageIndex) NullPage(int) bool   { return false }
func (index booleanPageIndex) MinValue(int) []byte { return plain.Boolean(index.page.min()) }
func (index booleanPageIndex) MaxValue(int) []byte { return plain.Boolean(index.page.max()) }
func (index booleanPageIndex) IsAscending() bool   { return compareBool(index.page.bounds()) < 0 }
func (index booleanPageIndex) IsDescending() bool  { return compareBool(index.page.bounds()) > 0 }

type int32PageIndex struct{ page *int32Page }

func (index int32PageIndex) NumPages() int       { return 1 }
func (index int32PageIndex) NullCount(int) int64 { return 0 }
func (index int32PageIndex) NullPage(int) bool   { return false }
func (index int32PageIndex) MinValue(int) []byte { return plain.Int32(index.page.min()) }
func (index int32PageIndex) MaxValue(int) []byte { return plain.Int32(index.page.max()) }
func (index int32PageIndex) IsAscending() bool   { return compareInt32(index.page.bounds()) < 0 }
func (index int32PageIndex) IsDescending() bool  { return compareInt32(index.page.bounds()) > 0 }

type int64PageIndex struct{ page *int64Page }

func (index int64PageIndex) NumPages() int       { return 1 }
func (index int64PageIndex) NullCount(int) int64 { return 0 }
func (index int64PageIndex) NullPage(int) bool   { return false }
func (index int64PageIndex) MinValue(int) []byte { return plain.Int64(index.page.min()) }
func (index int64PageIndex) MaxValue(int) []byte { return plain.Int64(index.page.max()) }
func (index int64PageIndex) IsAscending() bool   { return compareInt64(index.page.bounds()) < 0 }
func (index int64PageIndex) IsDescending() bool  { return compareInt64(index.page.bounds()) > 0 }

type int96PageIndex struct{ page *int96Page }

func (index int96PageIndex) NumPages() int       { return 1 }
func (index int96PageIndex) NullCount(int) int64 { return 0 }
func (index int96PageIndex) NullPage(int) bool   { return false }
func (index int96PageIndex) MinValue(int) []byte { return plain.Int96(index.page.min()) }
func (index int96PageIndex) MaxValue(int) []byte { return plain.Int96(index.page.max()) }
func (index int96PageIndex) IsAscending() bool   { return compareInt96(index.page.bounds()) < 0 }
func (index int96PageIndex) IsDescending() bool  { return compareInt96(index.page.bounds()) > 0 }

type floatPageIndex struct{ page *floatPage }

func (index floatPageIndex) NumPages() int       { return 1 }
func (index floatPageIndex) NullCount(int) int64 { return 0 }
func (index floatPageIndex) NullPage(int) bool   { return false }
func (index floatPageIndex) MinValue(int) []byte { return plain.Float(index.page.min()) }
func (index floatPageIndex) MaxValue(int) []byte { return plain.Float(index.page.max()) }
func (index floatPageIndex) IsAscending() bool   { return compareFloat32(index.page.bounds()) < 0 }
func (index floatPageIndex) IsDescending() bool  { return compareFloat32(index.page.bounds()) > 0 }

type doublePageIndex struct{ page *doublePage }

func (index doublePageIndex) NumPages() int       { return 1 }
func (index doublePageIndex) NullCount(int) int64 { return 0 }
func (index doublePageIndex) NullPage(int) bool   { return false }
func (index doublePageIndex) MinValue(int) []byte { return plain.Double(index.page.min()) }
func (index doublePageIndex) MaxValue(int) []byte { return plain.Double(index.page.max()) }
func (index doublePageIndex) IsAscending() bool   { return compareFloat64(index.page.bounds()) < 0 }
func (index doublePageIndex) IsDescending() bool  { return compareFloat64(index.page.bounds()) > 0 }

type byteArrayPageIndex struct{ page *byteArrayPage }

func (index byteArrayPageIndex) NumPages() int       { return 1 }
func (index byteArrayPageIndex) NullCount(int) int64 { return 0 }
func (index byteArrayPageIndex) NullPage(int) bool   { return false }
func (index byteArrayPageIndex) MinValue(int) []byte { return copyBytes(index.page.min()) }
func (index byteArrayPageIndex) MaxValue(int) []byte { return copyBytes(index.page.max()) }
func (index byteArrayPageIndex) IsAscending() bool   { return bytes.Compare(index.page.bounds()) < 0 }
func (index byteArrayPageIndex) IsDescending() bool  { return bytes.Compare(index.page.bounds()) > 0 }

type fixedLenByteArrayPageIndex struct{ page *fixedLenByteArrayPage }

func (index fixedLenByteArrayPageIndex) NumPages() int       { return 1 }
func (index fixedLenByteArrayPageIndex) NullCount(int) int64 { return 0 }
func (index fixedLenByteArrayPageIndex) NullPage(int) bool   { return false }
func (index fixedLenByteArrayPageIndex) MinValue(int) []byte { return copyBytes(index.page.min()) }
func (index fixedLenByteArrayPageIndex) MaxValue(int) []byte { return copyBytes(index.page.max()) }
func (index fixedLenByteArrayPageIndex) IsAscending() bool {
	return bytes.Compare(index.page.bounds()) < 0
}
func (index fixedLenByteArrayPageIndex) IsDescending() bool {
	return bytes.Compare(index.page.bounds()) > 0
}

type uint32PageIndex struct{ page uint32Page }

func (index uint32PageIndex) NumPages() int       { return 1 }
func (index uint32PageIndex) NullCount(int) int64 { return 0 }
func (index uint32PageIndex) NullPage(int) bool   { return false }
func (index uint32PageIndex) MinValue(int) []byte { return plain.Int32(int32(index.page.min())) }
func (index uint32PageIndex) MaxValue(int) []byte { return plain.Int32(int32(index.page.max())) }
func (index uint32PageIndex) IsAscending() bool   { return compareUint32(index.page.bounds()) < 0 }
func (index uint32PageIndex) IsDescending() bool  { return compareUint32(index.page.bounds()) > 0 }

type uint64PageIndex struct{ page uint64Page }

func (index uint64PageIndex) NumPages() int       { return 1 }
func (index uint64PageIndex) NullCount(int) int64 { return 0 }
func (index uint64PageIndex) NullPage(int) bool   { return false }
func (index uint64PageIndex) MinValue(int) []byte { return plain.Int64(int64(index.page.min())) }
func (index uint64PageIndex) MaxValue(int) []byte { return plain.Int64(int64(index.page.max())) }
func (index uint64PageIndex) IsAscending() bool   { return compareUint64(index.page.bounds()) < 0 }
func (index uint64PageIndex) IsDescending() bool  { return compareUint64(index.page.bounds()) > 0 }

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

type columnIndexer struct {
	nullPages  []bool
	nullCounts []int64
}

func (index *columnIndexer) reset() {
	index.nullPages = index.nullPages[:0]
	index.nullCounts = index.nullCounts[:0]
}

func (index *columnIndexer) observe(numValues, numNulls int64) {
	index.nullPages = append(index.nullPages, numValues == numNulls)
	index.nullCounts = append(index.nullCounts, numNulls)
}

func (index *columnIndexer) columnIndex(minValues, maxValues [][]byte, minOrder, maxOrder int) format.ColumnIndex {
	return format.ColumnIndex{
		NullPages:     index.nullPages,
		NullCounts:    index.nullCounts,
		MinValues:     minValues,
		MaxValues:     maxValues,
		BoundaryOrder: boundaryOrderOf(minOrder, maxOrder),
	}
}

type booleanColumnIndexer struct {
	columnIndexer
	minValues []bool
	maxValues []bool
}

func newBooleanColumnIndexer() *booleanColumnIndexer {
	return new(booleanColumnIndexer)
}

func (index *booleanColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *booleanColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Boolean())
	index.maxValues = append(index.maxValues, max.Boolean())
}

func (index *booleanColumnIndexer) ColumnIndex() format.ColumnIndex {
	return index.columnIndex(
		splitFixedLenByteArrayList(1, bits.BoolToBytes(index.minValues)),
		splitFixedLenByteArrayList(1, bits.BoolToBytes(index.maxValues)),
		bits.OrderOfBool(index.minValues),
		bits.OrderOfBool(index.maxValues),
	)
}

type int32ColumnIndexer struct {
	columnIndexer
	minValues []int32
	maxValues []int32
}

func newInt32ColumnIndexer() *int32ColumnIndexer {
	return new(int32ColumnIndexer)
}

func (index *int32ColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *int32ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Int32())
	index.maxValues = append(index.maxValues, max.Int32())
}

func (index *int32ColumnIndexer) ColumnIndex() format.ColumnIndex {
	return index.columnIndex(
		splitFixedLenByteArrayList(4, bits.Int32ToBytes(index.minValues)),
		splitFixedLenByteArrayList(4, bits.Int32ToBytes(index.maxValues)),
		bits.OrderOfInt32(index.minValues),
		bits.OrderOfInt32(index.maxValues),
	)
}

type int64ColumnIndexer struct {
	columnIndexer
	minValues []int64
	maxValues []int64
}

func newInt64ColumnIndexer() *int64ColumnIndexer {
	return new(int64ColumnIndexer)
}

func (index *int64ColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *int64ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Int64())
	index.maxValues = append(index.maxValues, max.Int64())
}

func (index *int64ColumnIndexer) ColumnIndex() format.ColumnIndex {
	return index.columnIndex(
		splitFixedLenByteArrayList(8, bits.Int64ToBytes(index.minValues)),
		splitFixedLenByteArrayList(8, bits.Int64ToBytes(index.maxValues)),
		bits.OrderOfInt64(index.minValues),
		bits.OrderOfInt64(index.maxValues),
	)
}

type int96ColumnIndexer struct {
	columnIndexer
	minValues []deprecated.Int96
	maxValues []deprecated.Int96
}

func newInt96ColumnIndexer() *int96ColumnIndexer {
	return new(int96ColumnIndexer)
}

func (index *int96ColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *int96ColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Int96())
	index.maxValues = append(index.maxValues, max.Int96())
}

func (index *int96ColumnIndexer) ColumnIndex() format.ColumnIndex {
	return index.columnIndex(
		splitFixedLenByteArrayList(12, deprecated.Int96ToBytes(index.minValues)),
		splitFixedLenByteArrayList(12, deprecated.Int96ToBytes(index.maxValues)),
		deprecated.OrderOfInt96(index.minValues),
		deprecated.OrderOfInt96(index.maxValues),
	)
}

type floatColumnIndexer struct {
	columnIndexer
	minValues []float32
	maxValues []float32
}

func newFloatColumnIndexer() *floatColumnIndexer {
	return new(floatColumnIndexer)
}

func (index *floatColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *floatColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Float())
	index.maxValues = append(index.maxValues, max.Float())
}

func (index *floatColumnIndexer) ColumnIndex() format.ColumnIndex {
	return index.columnIndex(
		splitFixedLenByteArrayList(4, bits.Float32ToBytes(index.minValues)),
		splitFixedLenByteArrayList(4, bits.Float32ToBytes(index.maxValues)),
		bits.OrderOfFloat32(index.minValues),
		bits.OrderOfFloat32(index.maxValues),
	)
}

type doubleColumnIndexer struct {
	columnIndexer
	minValues []float64
	maxValues []float64
}

func newDoubleColumnIndexer() *doubleColumnIndexer {
	return new(doubleColumnIndexer)
}

func (index *doubleColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *doubleColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Double())
	index.maxValues = append(index.maxValues, max.Double())
}

func (index *doubleColumnIndexer) ColumnIndex() format.ColumnIndex {
	return index.columnIndex(
		splitFixedLenByteArrayList(8, bits.Float64ToBytes(index.minValues)),
		splitFixedLenByteArrayList(8, bits.Float64ToBytes(index.maxValues)),
		bits.OrderOfFloat64(index.minValues),
		bits.OrderOfFloat64(index.maxValues),
	)
}

type byteArrayColumnIndexer struct {
	columnIndexer
	sizeLimit int
	minValues encoding.ByteArrayList
	maxValues encoding.ByteArrayList
}

func newByteArrayColumnIndexer(sizeLimit int) *byteArrayColumnIndexer {
	return &byteArrayColumnIndexer{sizeLimit: sizeLimit}
}

func (index *byteArrayColumnIndexer) Reset() {
	index.reset()
	index.minValues.Reset()
	index.maxValues.Reset()
}

func (index *byteArrayColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues.Push(min.ByteArray())
	index.maxValues.Push(max.ByteArray())
}

func (index *byteArrayColumnIndexer) ColumnIndex() format.ColumnIndex {
	minValues := index.minValues.Split()
	maxValues := index.maxValues.Split()
	if index.sizeLimit > 0 {
		truncateLargeMinByteArrayValues(minValues, index.sizeLimit)
		truncateLargeMaxByteArrayValues(maxValues, index.sizeLimit)
	}
	return index.columnIndex(
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
	columnIndexer
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

func (index *fixedLenByteArrayColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *fixedLenByteArrayColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.ByteArray()...)
	index.maxValues = append(index.maxValues, max.ByteArray()...)
}

func (index *fixedLenByteArrayColumnIndexer) ColumnIndex() format.ColumnIndex {
	minValues := splitFixedLenByteArrayList(index.size, index.minValues)
	maxValues := splitFixedLenByteArrayList(index.size, index.maxValues)
	if index.sizeLimit > 0 && index.sizeLimit < index.size {
		truncateLargeMinByteArrayValues(minValues, index.sizeLimit)
		truncateLargeMaxByteArrayValues(maxValues, index.sizeLimit)
	}
	return index.columnIndex(
		minValues,
		maxValues,
		bits.OrderOfBytes(minValues),
		bits.OrderOfBytes(maxValues),
	)
}

type uint32ColumnIndexer struct{ *int32ColumnIndexer }

func newUint32ColumnIndexer() uint32ColumnIndexer {
	return uint32ColumnIndexer{newInt32ColumnIndexer()}
}

func (index uint32ColumnIndexer) ColumnIndex() format.ColumnIndex {
	minValues := bits.Int32ToUint32(index.minValues)
	maxValues := bits.Int32ToUint32(index.maxValues)
	return index.columnIndex(
		splitFixedLenByteArrayList(4, bits.Uint32ToBytes(minValues)),
		splitFixedLenByteArrayList(4, bits.Uint32ToBytes(maxValues)),
		bits.OrderOfUint32(minValues),
		bits.OrderOfUint32(maxValues),
	)
}

type uint64ColumnIndexer struct{ *int64ColumnIndexer }

func newUint64ColumnIndexer() uint64ColumnIndexer {
	return uint64ColumnIndexer{newInt64ColumnIndexer()}
}

func (index uint64ColumnIndexer) ColumnIndex() format.ColumnIndex {
	minValues := bits.Int64ToUint64(index.minValues)
	maxValues := bits.Int64ToUint64(index.maxValues)
	return index.columnIndex(
		splitFixedLenByteArrayList(8, bits.Uint64ToBytes(minValues)),
		splitFixedLenByteArrayList(8, bits.Uint64ToBytes(maxValues)),
		bits.OrderOfUint64(minValues),
		bits.OrderOfUint64(maxValues),
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
