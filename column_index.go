package parquet

import (
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
	MinValue(int) Value
	MaxValue(int) Value

	// IsAscending returns true if the column index min/max values are sorted
	// in ascending order (based on the ordering rules of the column's logical
	// type).
	IsAscending() bool

	// IsDescending returns true if the column index min/max values are sorted
	// in descending order (based on the ordering rules of the column's logical
	// type).
	IsDescending() bool
}

// NewColumnIndex constructs a ColumnIndex instance from the given parquet
// format column index. The kind argument configures the type of values
func NewColumnIndex(kind Kind, index *format.ColumnIndex) ColumnIndex {
	return &formatColumnIndex{
		kind:  kind,
		index: index,
	}
}

type formatColumnIndex struct {
	kind  Kind
	index *format.ColumnIndex
}

func (f *formatColumnIndex) NumPages() int {
	return len(f.index.MinValues)
}

func (f *formatColumnIndex) NullCount(i int) int64 {
	if len(f.index.NullCounts) > 0 {
		return f.index.NullCounts[i]
	}
	return 0
}

func (f *formatColumnIndex) NullPage(i int) bool {
	return len(f.index.NullPages) > 0 && f.index.NullPages[i]
}

func (f *formatColumnIndex) MinValue(i int) Value {
	if f.NullPage(i) {
		return Value{}
	}
	return f.kind.Value(f.index.MinValues[i])
}

func (f *formatColumnIndex) MaxValue(i int) Value {
	if f.NullPage(i) {
		return Value{}
	}
	return f.kind.Value(f.index.MaxValues[i])
}

func (f *formatColumnIndex) IsAscending() bool {
	return f.index.BoundaryOrder == format.Ascending
}

func (f *formatColumnIndex) IsDescending() bool {
	return f.index.BoundaryOrder == format.Descending
}

type emptyColumnIndex struct{}

func (emptyColumnIndex) NumPages() int       { return 0 }
func (emptyColumnIndex) NullCount(int) int64 { return 0 }
func (emptyColumnIndex) NullPage(int) bool   { return false }
func (emptyColumnIndex) MinValue(int) Value  { return Value{} }
func (emptyColumnIndex) MaxValue(int) Value  { return Value{} }
func (emptyColumnIndex) IsAscending() bool   { return false }
func (emptyColumnIndex) IsDescending() bool  { return false }

type fileColumnIndex struct{ chunk *fileColumnChunk }

func (i fileColumnIndex) NumPages() int {
	return len(i.chunk.columnIndex.NullPages)
}

func (i fileColumnIndex) NullCount(j int) int64 {
	if len(i.chunk.columnIndex.NullCounts) > 0 {
		return i.chunk.columnIndex.NullCounts[j]
	}
	return 0
}

func (i fileColumnIndex) NullPage(j int) bool {
	return len(i.chunk.columnIndex.NullPages) > 0 && i.chunk.columnIndex.NullPages[j]
}

func (i fileColumnIndex) MinValue(j int) Value {
	if i.NullPage(j) {
		return Value{}
	}
	return i.makeValue(i.chunk.columnIndex.MinValues[j])
}

func (i fileColumnIndex) MaxValue(j int) Value {
	if i.NullPage(j) {
		return Value{}
	}
	return i.makeValue(i.chunk.columnIndex.MaxValues[j])
}

func (i fileColumnIndex) IsAscending() bool {
	return i.chunk.columnIndex.BoundaryOrder == format.Ascending
}

func (i fileColumnIndex) IsDescending() bool {
	return i.chunk.columnIndex.BoundaryOrder == format.Descending
}

func (i *fileColumnIndex) makeValue(b []byte) Value {
	return i.chunk.column.typ.Kind().Value(b)
}

type byteArrayColumnIndex struct{ page *byteArrayPage }

func (i byteArrayColumnIndex) NumPages() int       { return 1 }
func (i byteArrayColumnIndex) NullCount(int) int64 { return 0 }
func (i byteArrayColumnIndex) NullPage(int) bool   { return false }
func (i byteArrayColumnIndex) MinValue(int) Value  { return makeValueBytes(ByteArray, i.page.min()) }
func (i byteArrayColumnIndex) MaxValue(int) Value  { return makeValueBytes(ByteArray, i.page.max()) }
func (i byteArrayColumnIndex) IsAscending() bool   { return false }
func (i byteArrayColumnIndex) IsDescending() bool  { return false }

type fixedLenByteArrayColumnIndex struct{ page *fixedLenByteArrayPage }

func (i fixedLenByteArrayColumnIndex) NumPages() int       { return 1 }
func (i fixedLenByteArrayColumnIndex) NullCount(int) int64 { return 0 }
func (i fixedLenByteArrayColumnIndex) NullPage(int) bool   { return false }
func (i fixedLenByteArrayColumnIndex) MinValue(int) Value {
	return makeValueBytes(FixedLenByteArray, i.page.min())
}
func (i fixedLenByteArrayColumnIndex) MaxValue(int) Value {
	return makeValueBytes(FixedLenByteArray, i.page.max())
}
func (i fixedLenByteArrayColumnIndex) IsAscending() bool  { return false }
func (i fixedLenByteArrayColumnIndex) IsDescending() bool { return false }

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
	minValues []byte
	maxValues []byte
}

func newByteArrayColumnIndexer(sizeLimit int) *byteArrayColumnIndexer {
	return &byteArrayColumnIndexer{sizeLimit: sizeLimit}
}

func (i *byteArrayColumnIndexer) Reset() {
	i.reset()
	i.minValues = i.minValues[:0]
	i.maxValues = i.maxValues[:0]
}

func (i *byteArrayColumnIndexer) IndexPage(numValues, numNulls int64, min, max Value) {
	i.observe(numValues, numNulls)
	minValue := min.ByteArray()
	maxValue := max.ByteArray()
	if i.sizeLimit > 0 {
		minValue = truncateLargeMinByteArrayValue(minValue, i.sizeLimit)
		maxValue = truncateLargeMaxByteArrayValue(maxValue, i.sizeLimit)
	}
	i.minValues = plain.AppendByteArray(i.minValues, minValue)
	i.maxValues = plain.AppendByteArray(i.maxValues, maxValue)
}

func (i *byteArrayColumnIndexer) ColumnIndex() format.ColumnIndex {
	minValues := splitByteArrays(i.minValues)
	maxValues := splitByteArrays(i.maxValues)
	return i.columnIndex(
		minValues,
		maxValues,
		bits.OrderOfBytes(minValues),
		bits.OrderOfBytes(maxValues),
	)
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
	minValues := splitFixedLenByteArrays(i.minValues, i.size)
	maxValues := splitFixedLenByteArrays(i.maxValues, i.size)
	if sizeLimit := i.sizeLimit; sizeLimit > 0 {
		for i, v := range minValues {
			minValues[i] = truncateLargeMinByteArrayValue(v, sizeLimit)
		}
		for i, v := range maxValues {
			maxValues[i] = truncateLargeMaxByteArrayValue(v, sizeLimit)
		}
	}
	return i.columnIndex(
		minValues,
		maxValues,
		bits.OrderOfBytes(minValues),
		bits.OrderOfBytes(maxValues),
	)
}

func truncateLargeMinByteArrayValue(value []byte, sizeLimit int) []byte {
	if len(value) > sizeLimit {
		value = value[:sizeLimit]
	}
	return value
}

func truncateLargeMaxByteArrayValue(value []byte, sizeLimit int) []byte {
	if len(value) > sizeLimit && !isMaxByteArrayValue(value) {
		value = value[:sizeLimit]
	}
	return value
}

func isMaxByteArrayValue(value []byte) bool {
	for i := range value {
		if value[i] != 0xFF {
			return false
		}
	}
	return true
}

func splitByteArrays(data []byte) [][]byte {
	length := 0
	plain.RangeByteArrays(data, func([]byte) error {
		length++
		return nil
	})
	buffer := make([]byte, 0, len(data)-(4*length))
	values := make([][]byte, 0, length)
	plain.RangeByteArrays(data, func(value []byte) error {
		offset := len(buffer)
		buffer = append(buffer, value...)
		values = append(values, buffer[offset:])
		return nil
	})
	return values
}

func splitFixedLenByteArrays(data []byte, size int) [][]byte {
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
