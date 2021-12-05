package parquet

import (
	"bytes"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

// ColumnIndex is the data structure representing column indexes.
type ColumnIndex format.ColumnIndex

// PageBounds return min/max bounds for the page at index i in the column index.
// The last returned value is a boolean indicating whether the page only
// contained null values, in which case the min/max values are empty byte
// slices which must be interpreted as the null parquet value.
func (index *ColumnIndex) PageBounds(i int) (minValue, maxValue []byte, nullPage bool) {
	minValue = index.MinValues[i]
	maxValue = index.MaxValues[i]
	nullPage = index.NullPages[i]
	return
}

// PageNulls returns the number of null values in the page at index i.
func (index *ColumnIndex) PageNulls(i int) int64 { return index.NullCounts[i] }

// NumPages returns the number of paged in the column index.
func (index *ColumnIndex) NumPages() int { return len(index.NullPages) }

// IsAscending returns true if the column index min/max values are sorted in
// ascending order (based on the ordering rules of the column's logical type).
func (index *ColumnIndex) IsAscending() bool { return index.BoundaryOrder == format.Ascending }

// IsDescending returns true if the column index min/max values are sorted in
// descending order (based on the ordering rules of the column's logical type).
func (index *ColumnIndex) IsDescending() bool { return index.BoundaryOrder == format.Descending }

// The ColumnIndexer interface is implemented by types that support generating
// parquet column indexes.
//
// The package does not export any types that implement this interface, programs
// must call NewColumnIndexer on a Type instance to construct column indexers.
type ColumnIndexer interface {
	// Returns the type of values being indexed.
	Type() Type

	// Resets the column indexer state.
	Reset()

	// Returns the min and max values that have been indexed.
	Bounds() (min, max Value)

	// Add a page to the column indexer.
	IndexPage(numValues, numNulls int, min, max Value)

	// Generates a format.ColumnIndex value from the current state of the
	// column indexer.
	//
	// The returned value may reference internal buffers, in which case the
	// values remain valid until the next call to IndexPage or Reset on the
	// column indexer.
	ColumnIndex() ColumnIndex
}

type columnIndexer struct {
	typ        Type
	nullPages  []bool
	nullCounts []int64
}

func (index *columnIndexer) Type() Type {
	return index.typ
}

func (index *columnIndexer) reset() {
	index.nullPages = index.nullPages[:0]
	index.nullCounts = index.nullCounts[:0]
}

func (index *columnIndexer) observe(numValues, numNulls int) {
	index.nullPages = append(index.nullPages, numValues == numNulls)
	index.nullCounts = append(index.nullCounts, int64(numNulls))
}

func (index *columnIndexer) columnIndex(minValues, maxValues [][]byte, minOrder, maxOrder int) ColumnIndex {
	return ColumnIndex{
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

func newBooleanColumnIndexer(typ Type) *booleanColumnIndexer {
	return &booleanColumnIndexer{columnIndexer: columnIndexer{typ: typ}}
}

func (index *booleanColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *booleanColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = makeValueBoolean(bits.MinBool(index.minValues))
		max = makeValueBoolean(bits.MaxBool(index.maxValues))
	}
	return min, max
}

func (index *booleanColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Boolean())
	index.maxValues = append(index.maxValues, max.Boolean())
}

func (index *booleanColumnIndexer) ColumnIndex() ColumnIndex {
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

func newInt32ColumnIndexer(typ Type) *int32ColumnIndexer {
	return &int32ColumnIndexer{columnIndexer: columnIndexer{typ: typ}}
}

func (index *int32ColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *int32ColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = makeValueInt32(bits.MinInt32(index.minValues))
		max = makeValueInt32(bits.MaxInt32(index.maxValues))
	}
	return min, max
}

func (index *int32ColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Int32())
	index.maxValues = append(index.maxValues, max.Int32())
}

func (index *int32ColumnIndexer) ColumnIndex() ColumnIndex {
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

func newInt64ColumnIndexer(typ Type) *int64ColumnIndexer {
	return &int64ColumnIndexer{columnIndexer: columnIndexer{typ: typ}}
}

func (index *int64ColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *int64ColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = makeValueInt64(bits.MinInt64(index.minValues))
		max = makeValueInt64(bits.MaxInt64(index.maxValues))
	}
	return min, max
}

func (index *int64ColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Int64())
	index.maxValues = append(index.maxValues, max.Int64())
}

func (index *int64ColumnIndexer) ColumnIndex() ColumnIndex {
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

func newInt96ColumnIndexer(typ Type) *int96ColumnIndexer {
	return &int96ColumnIndexer{columnIndexer: columnIndexer{typ: typ}}
}

func (index *int96ColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *int96ColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = makeValueInt96(deprecated.MinInt96(index.minValues))
		max = makeValueInt96(deprecated.MaxInt96(index.maxValues))
	}
	return min, max
}

func (index *int96ColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Int96())
	index.maxValues = append(index.maxValues, max.Int96())
}

func (index *int96ColumnIndexer) ColumnIndex() ColumnIndex {
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

func newFloatColumnIndexer(typ Type) *floatColumnIndexer {
	return &floatColumnIndexer{columnIndexer: columnIndexer{typ: typ}}
}

func (index *floatColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *floatColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = makeValueFloat(bits.MinFloat32(index.minValues))
		max = makeValueFloat(bits.MaxFloat32(index.maxValues))
	}
	return min, max
}

func (index *floatColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Float())
	index.maxValues = append(index.maxValues, max.Float())
}

func (index *floatColumnIndexer) ColumnIndex() ColumnIndex {
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

func newDoubleColumnIndexer(typ Type) *doubleColumnIndexer {
	return &doubleColumnIndexer{columnIndexer: columnIndexer{typ: typ}}
}

func (index *doubleColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *doubleColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = makeValueDouble(bits.MinFloat64(index.minValues))
		max = makeValueDouble(bits.MaxFloat64(index.maxValues))
	}
	return min, max
}

func (index *doubleColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Double())
	index.maxValues = append(index.maxValues, max.Double())
}

func (index *doubleColumnIndexer) ColumnIndex() ColumnIndex {
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
	minValues []byte
	maxValues []byte
	min       []byte
	max       []byte
}

func newByteArrayColumnIndexer(typ Type, sizeLimit int) *byteArrayColumnIndexer {
	return &byteArrayColumnIndexer{
		columnIndexer: columnIndexer{typ: typ},
		sizeLimit:     sizeLimit,
	}
}

func (index *byteArrayColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
	index.min = index.min[:0]
	index.max = index.max[:0]
}

func (index *byteArrayColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = makeValueBytes(ByteArray, index.min)
		max = makeValueBytes(ByteArray, index.max)
	}
	return min, max
}

func (index *byteArrayColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	minValue := min.ByteArray()
	maxValue := max.ByteArray()

	if len(index.minValues) == 0 {
		index.setMin(minValue)
		index.setMax(maxValue)
	} else {
		if bytes.Compare(minValue, index.min) < 0 {
			index.setMin(minValue)
		}
		if bytes.Compare(maxValue, index.max) > 0 {
			index.setMax(maxValue)
		}
	}

	index.minValues = plain.AppendByteArray(index.minValues, minValue)
	index.maxValues = plain.AppendByteArray(index.maxValues, maxValue)
}

func (index *byteArrayColumnIndexer) setMin(min []byte) {
	index.min = append(index.min[:0], min...)
}

func (index *byteArrayColumnIndexer) setMax(max []byte) {
	index.max = append(index.max[:0], max...)
}

func (index *byteArrayColumnIndexer) ColumnIndex() ColumnIndex {
	// It is safe to ignore the errors here because we know the input is a
	// valid PLAIN encoded list of byte array values.
	minValues, _ := plain.SplitByteArrayList(index.minValues)
	maxValues, _ := plain.SplitByteArrayList(index.maxValues)
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

func newFixedLenByteArrayColumnIndexer(typ Type, sizeLimit int) *fixedLenByteArrayColumnIndexer {
	return &fixedLenByteArrayColumnIndexer{
		columnIndexer: columnIndexer{typ: typ},
		size:          typ.Length(),
		sizeLimit:     sizeLimit,
	}
}

func (index *fixedLenByteArrayColumnIndexer) Reset() {
	index.reset()
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *fixedLenByteArrayColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = makeValueBytes(FixedLenByteArray, bits.MinFixedLenByteArray(index.size, index.minValues))
		max = makeValueBytes(FixedLenByteArray, bits.MaxFixedLenByteArray(index.size, index.maxValues))
	}
	return min, max
}

func (index *fixedLenByteArrayColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.ByteArray()...)
	index.maxValues = append(index.maxValues, max.ByteArray()...)
}

func (index *fixedLenByteArrayColumnIndexer) ColumnIndex() ColumnIndex {
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

func newUint32ColumnIndexer(typ Type) uint32ColumnIndexer {
	return uint32ColumnIndexer{newInt32ColumnIndexer(typ)}
}

func (index uint32ColumnIndexer) ColumnIndex() ColumnIndex {
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

func newUint64ColumnIndexer(typ Type) uint64ColumnIndexer {
	return uint64ColumnIndexer{newInt64ColumnIndexer(typ)}
}

func (index uint64ColumnIndexer) ColumnIndex() ColumnIndex {
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
