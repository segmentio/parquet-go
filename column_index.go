package parquet

import (
	"bytes"

	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

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
	ColumnIndex() format.ColumnIndex
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

func (index *columnIndexer) columnIndex(minValues, maxValues [][]byte, minOrder, maxOrder int) format.ColumnIndex {
	return format.ColumnIndex{
		NullPages:     index.nullPages,
		NullCounts:    index.nullCounts,
		MinValues:     minValues,
		MaxValues:     maxValues,
		BoundaryOrder: boundaryOrderOf(minOrder, maxOrder),
	}
}

type genericColumnIndexer struct {
	columnIndexer
	minValues []Value
	maxValues []Value
}

func newGenericColumnIndexer(typ Type) *genericColumnIndexer {
	return &genericColumnIndexer{columnIndexer: columnIndexer{typ: typ}}
}

func (index *genericColumnIndexer) Reset() {
	index.reset()
	clearValues(index.minValues)
	clearValues(index.maxValues)
	index.minValues = index.minValues[:0]
	index.maxValues = index.maxValues[:0]
}

func (index *genericColumnIndexer) Bounds() (min, max Value) {
	if len(index.minValues) > 0 {
		min = minValueOf(index.typ, index.minValues)
		max = maxValueOf(index.typ, index.maxValues)
	}
	return min, max
}

func (index *genericColumnIndexer) IndexPage(numValues, numNulls int, min, max Value) {
	index.observe(numValues, numNulls)
	index.minValues = append(index.minValues, min.Clone())
	index.maxValues = append(index.maxValues, max.Clone())
}

func (index *genericColumnIndexer) ColumnIndex() format.ColumnIndex {
	return index.columnIndex(
		valuesBytes(index.typ, index.minValues),
		valuesBytes(index.typ, index.maxValues),
		orderOfValues(index.typ, index.minValues),
		orderOfValues(index.typ, index.maxValues),
	)
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

func (index *int64ColumnIndexer) ColumnIndex() format.ColumnIndex {
	return index.columnIndex(
		splitFixedLenByteArrayList(8, bits.Int64ToBytes(index.minValues)),
		splitFixedLenByteArrayList(8, bits.Int64ToBytes(index.maxValues)),
		bits.OrderOfInt64(index.minValues),
		bits.OrderOfInt64(index.maxValues),
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
	minValues []byte
	maxValues []byte
	min       []byte
	max       []byte
}

func newByteArrayColumnIndexer(typ Type) *byteArrayColumnIndexer {
	return &byteArrayColumnIndexer{columnIndexer: columnIndexer{typ: typ}}
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

func (index *byteArrayColumnIndexer) ColumnIndex() format.ColumnIndex {
	// It is safe to ignore the errors here because we know the input is a
	// valid PLAIN encoded list of byte array values.
	minValues, _ := plain.SplitByteArrayList(index.minValues)
	maxValues, _ := plain.SplitByteArrayList(index.maxValues)
	truncateLargeMinByteArrayValues(minValues)
	truncateLargeMaxByteArrayValues(maxValues)
	return index.columnIndex(
		minValues,
		maxValues,
		bits.OrderOfBytes(minValues),
		bits.OrderOfBytes(maxValues),
	)
}

const (
	// TODO: come up with a way to configure this?
	maxValueSize = 64
)

func truncateLargeMinByteArrayValues(values [][]byte) {
	for i, v := range values {
		if len(v) > maxValueSize {
			values[i] = v[:maxValueSize]
		}
	}
}

func truncateLargeMaxByteArrayValues(values [][]byte) {
	for i, v := range values {
		if len(v) > maxValueSize {
			// If v is the max value we cannot truncate it since there are no
			// shorter byte sequence with a greater value. This condition should
			// never occur unless the input was especially constructed to trigger
			// it.
			if !isMaxByteArrayValue(v) {
				values[i] = nextByteArrayValue(v[:maxValueSize])
			}
		}
	}
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
	next := make([]byte, len(value))
	copy(next, value)
	for i := len(b) - 1; i > 0; i-- {
		if next[i]++; next[i] != 0 {
			break
		}
		// Overflow: increment the next byte
	}
	return next
}

type fixedLenByteArrayColumnIndexer struct {
	columnIndexer
	size      int
	minValues []byte
	maxValues []byte
}

func newFixedLenByteArrayColumnIndexer(typ Type) *fixedLenByteArrayColumnIndexer {
	return &fixedLenByteArrayColumnIndexer{
		columnIndexer: columnIndexer{typ: typ},
		size:          typ.Length(),
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

func (index *fixedLenByteArrayColumnIndexer) ColumnIndex() format.ColumnIndex {
	minValues := splitFixedLenByteArrayList(index.size, index.minValues)
	maxValues := splitFixedLenByteArrayList(index.size, index.maxValues)
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

func newUint64ColumnIndexer(typ Type) uint64ColumnIndexer {
	return uint64ColumnIndexer{newInt64ColumnIndexer(typ)}
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
	values := make([][]byte, len(data)/size)
	for i := range values {
		j := (i + 0) * size
		k := (i + 1) * size
		values[i] = data[j:k]
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

func valuesBytes(typ Type, values []Value) [][]byte {
	valuesBytes := make([][]byte, len(values))

	switch typ.Kind() {
	case ByteArray, FixedLenByteArray:
		// For BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY, the parquet value already
		// references an underlying byte array. We don't have to copy it to a
		// new buffer and can simply reference it in the returned slice.
		for i, v := range values {
			valuesBytes[i] = v.ByteArray()
		}

	default:
		buffer := make([]byte, 0, typeSizeOf(typ)*len(values))

		for i, v := range values {
			offset := len(buffer)
			buffer = v.AppendBytes(buffer)
			valuesBytes[i] = buffer[offset:len(buffer):len(buffer)]
		}
	}

	return valuesBytes
}

func minValueOf(typ Type, values []Value) (min Value) {
	if len(values) > 0 {
		min = values[0]
		for _, value := range values[1:] {
			if typ.Less(value, min) {
				min = value
			}
		}
	}
	return min
}

func maxValueOf(typ Type, values []Value) (max Value) {
	if len(values) > 0 {
		max = values[0]
		for _, value := range values[1:] {
			if typ.Less(max, value) {
				max = value
			}
		}
	}
	return max
}

func orderOfValues(typ Type, values []Value) int {
	if len(values) > 0 {
		if valuesAreInAscendingOrder(typ, values) {
			return +1
		}
		if valuesAreInDescendingOrder(typ, values) {
			return -1
		}
	}
	return 0
}

func valuesAreInAscendingOrder(typ Type, values []Value) bool {
	for i := len(values) - 1; i > 0; i-- {
		if typ.Less(values[i], values[i-1]) {
			return false
		}
	}
	return true
}

func valuesAreInDescendingOrder(typ Type, values []Value) bool {
	for i := len(values) - 1; i > 0; i-- {
		if typ.Less(values[i-1], values[i]) {
			return false
		}
	}
	return true
}

func clearValues(values []Value) {
	for i := range values {
		values[i] = Value{}
	}
}
