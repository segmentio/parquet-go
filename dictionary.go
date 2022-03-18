package parquet

import (
	"bytes"
	"fmt"
	"io"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/internal/bits"
)

const (
	// Completely arbitrary, feel free to adjust if a different value would be
	// more representative of the map implementation in Go.
	mapSizeOverheadPerItem = 8
)

// The Dictionary interface represents type-specific implementations of parquet
// dictionaries.
//
// Programs can instantiate dictionaries by call the NewDictionary method of a
// Type object.
type Dictionary interface {
	// Returns the type that the dictionary was created from.
	Type() Type

	// Returns the number of value indexed in the dictionary.
	Len() int

	// Returns the dictionary value at the given index.
	Index(int) Value

	// Inserts a value to the dictionary, returning the index at which it was
	// recorded.
	Insert(Value) int

	// Given an array of dictionary indexes, lookup the values into the array
	// of values passed as second argument.
	//
	// The method panics if len(indexes) > len(values), or one of the indexes
	// is negative or greater than the highest index in the dictionary.
	Lookup(indexes []int32, values []Value)

	// Returns the min and max values found in the given indexes.
	Bounds(indexed []int32) (min, max Value)

	// Reads the dictionary from the decoder passed as argument.
	//
	// The dictionary is cleared prior to loading the values so that its final
	// content contains only the entries read from the decoder.
	ReadFrom(encoding.Decoder) error

	// Writes the dictionary to the encoder passed as argument.
	WriteTo(encoding.Encoder) error

	// Resets the dictionary to its initial state, removing all values.
	Reset()
}

func dictCap(bufferSize, valueItemSize int) int {
	indexItemSize := 4 + valueItemSize + mapSizeOverheadPerItem
	return atLeastOne(bufferSize / (valueItemSize + indexItemSize))
}

type byteArrayDictionary struct {
	typ    Type
	values encoding.ByteArrayList
	index  map[string]int32
}

func newByteArrayDictionary(typ Type, bufferSize int) *byteArrayDictionary {
	return &byteArrayDictionary{
		typ:    typ,
		values: encoding.MakeByteArrayList(dictCap(bufferSize, 16)),
	}
}

func (d *byteArrayDictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *byteArrayDictionary) Len() int { return d.values.Len() }

func (d *byteArrayDictionary) Index(i int) Value { return makeValueBytes(ByteArray, d.values.Index(i)) }

func (d *byteArrayDictionary) Insert(v Value) int { return d.insert(v.ByteArray()) }

func (d *byteArrayDictionary) insert(value []byte) int {
	if index, exists := d.index[string(value)]; exists {
		return int(index)
	}
	d.values.Push(value)
	index := d.values.Len() - 1
	stringValue := bits.BytesToString(d.values.Index(index))
	if d.index == nil {
		d.index = make(map[string]int32, d.values.Cap())
	}
	d.index[stringValue] = int32(index)
	return index
}

func (d *byteArrayDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

func (d *byteArrayDictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.values.Index(int(indexes[0]))
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := d.values.Index(int(i))
			switch {
			case bytes.Compare(value, minValue) < 0:
				minValue = value
			case bytes.Compare(value, maxValue) > 0:
				maxValue = value
			}
		}

		min = makeValueBytes(ByteArray, minValue)
		max = makeValueBytes(ByteArray, maxValue)
	}
	return min, max
}

func (d *byteArrayDictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()
	for {
		if d.values.Len() == d.values.Cap() {
			d.values.Grow(d.values.Len())
		}
		_, err := decoder.DecodeByteArray(&d.values)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
	}
}

func (d *byteArrayDictionary) WriteTo(encoder encoding.Encoder) error {
	if err := encoder.EncodeByteArray(d.values); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d binary values: %w", d.Len(), err)
	}
	return nil
}

func (d *byteArrayDictionary) Reset() {
	d.values.Reset()
	d.index = nil
}

type fixedLenByteArrayDictionary struct {
	typ    Type
	size   int
	values []byte
	index  map[string]int32
}

func newFixedLenByteArrayDictionary(typ Type, bufferSize int) *fixedLenByteArrayDictionary {
	size := typ.Length()
	return &fixedLenByteArrayDictionary{
		typ:    typ,
		size:   size,
		values: make([]byte, 0, dictCap(bufferSize, size)),
	}
}

func (d *fixedLenByteArrayDictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *fixedLenByteArrayDictionary) Len() int { return len(d.values) / d.size }

func (d *fixedLenByteArrayDictionary) Index(i int) Value {
	return makeValueBytes(FixedLenByteArray, d.value(i))
}

func (d *fixedLenByteArrayDictionary) value(i int) []byte {
	return d.values[i*d.size : (i+1)*d.size]
}

func (d *fixedLenByteArrayDictionary) Insert(v Value) int {
	return d.insert(v.ByteArray())
}

func (d *fixedLenByteArrayDictionary) insert(value []byte) int {
	if index, exists := d.index[string(value)]; exists {
		return int(index)
	}
	if d.index == nil {
		d.index = make(map[string]int32, cap(d.values)/d.size)
	}
	i := d.Len()
	n := len(d.values)
	d.values = append(d.values, value...)
	d.index[bits.BytesToString(d.values[n:])] = int32(i)
	return i
}

func (d *fixedLenByteArrayDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

func (d *fixedLenByteArrayDictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.value(int(indexes[0]))
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := d.value(int(i))
			switch {
			case bytes.Compare(value, minValue) < 0:
				minValue = value
			case bytes.Compare(value, maxValue) > 0:
				maxValue = value
			}
		}

		min = makeValueBytes(FixedLenByteArray, minValue)
		max = makeValueBytes(FixedLenByteArray, maxValue)
	}
	return min, max
}

func (d *fixedLenByteArrayDictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()
	for {
		if len(d.values) == cap(d.values) {
			newValues := make([]byte, len(d.values), 2*cap(d.values))
			copy(newValues, d.values)
			d.values = newValues
		}

		n, err := decoder.DecodeFixedLenByteArray(d.size, d.values[len(d.values):cap(d.values)])
		if n > 0 {
			d.values = d.values[:len(d.values)+(n*d.size)]
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("reading parquet dictionary of fixed-length binary values of size %d: %w", d.size, err)
			}
			return err
		}
	}
}

func (d *fixedLenByteArrayDictionary) WriteTo(encoder encoding.Encoder) error {
	if err := encoder.EncodeFixedLenByteArray(d.size, d.values); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d fixed-length binary values of size %d: %w", d.Len(), d.size, err)
	}
	return nil
}

func (d *fixedLenByteArrayDictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

type indexedType struct {
	Type
	dict Dictionary
}

func newIndexedType(typ Type, dict Dictionary) *indexedType {
	return &indexedType{Type: typ, dict: dict}
}

func (t *indexedType) NewColumnBuffer(columnIndex, bufferSize int) ColumnBuffer {
	return newIndexedColumnBuffer(t.dict, t, makeColumnIndex(columnIndex), bufferSize)
}

func (t *indexedType) NewColumnReader(columnIndex, bufferSize int) ColumnReader {
	return newIndexedColumnReader(t.dict, t, makeColumnIndex(columnIndex), bufferSize)
}

type indexedPage struct {
	dict        Dictionary
	values      []int32
	columnIndex int16
}

func (page *indexedPage) Column() int { return int(^page.columnIndex) }

func (page *indexedPage) Dictionary() Dictionary { return page.dict }

func (page *indexedPage) NumRows() int64 { return int64(len(page.values)) }

func (page *indexedPage) NumValues() int64 { return int64(len(page.values)) }

func (page *indexedPage) NumNulls() int64 { return 0 }

func (page *indexedPage) Bounds() (min, max Value) {
	min, max = page.dict.Bounds(page.values)
	min.columnIndex = page.columnIndex
	max.columnIndex = page.columnIndex
	return min, max
}

func (page *indexedPage) Clone() BufferedPage {
	return &indexedPage{
		dict:        page.dict,
		values:      append([]int32{}, page.values...),
		columnIndex: page.columnIndex,
	}
}

func (page *indexedPage) Slice(i, j int64) BufferedPage {
	return &indexedPage{
		dict:        page.dict,
		values:      page.values[i:j],
		columnIndex: page.columnIndex,
	}
}

func (page *indexedPage) Size() int64 { return sizeOfInt32(page.values) }

func (page *indexedPage) RepetitionLevels() []int8 { return nil }

func (page *indexedPage) DefinitionLevels() []int8 { return nil }

func (page *indexedPage) WriteTo(e encoding.Encoder) error { return e.EncodeInt32(page.values) }

func (page *indexedPage) Values() ValueReader { return &indexedPageReader{page: page} }

func (page *indexedPage) Buffer() BufferedPage { return page }

type indexedPageReader struct {
	page   *indexedPage
	offset int
}

func (r *indexedPageReader) ReadValues(values []Value) (n int, err error) {
	var v Value
	for n < len(values) && r.offset < len(r.page.values) {
		v = r.page.dict.Index(int(r.page.values[r.offset]))
		v.columnIndex = r.page.columnIndex
		values[n] = v
		r.offset++
		n++
	}
	if r.offset == len(r.page.values) {
		err = io.EOF
	}

	return n, err
}

type indexedColumnBuffer struct {
	indexedPage
	typ Type
}

func newIndexedColumnBuffer(dict Dictionary, typ Type, columnIndex int16, bufferSize int) *indexedColumnBuffer {
	return &indexedColumnBuffer{
		indexedPage: indexedPage{
			dict:        dict,
			values:      make([]int32, 0, bufferSize/4),
			columnIndex: ^columnIndex,
		},
		typ: typ,
	}
}

func (col *indexedColumnBuffer) Clone() ColumnBuffer {
	return &indexedColumnBuffer{
		indexedPage: indexedPage{
			dict:        col.dict,
			values:      append([]int32{}, col.values...),
			columnIndex: col.columnIndex,
		},
		typ: col.typ,
	}
}

func (col *indexedColumnBuffer) Type() Type { return col.typ }

func (col *indexedColumnBuffer) ColumnIndex() ColumnIndex { return indexedColumnIndex{col} }

func (col *indexedColumnBuffer) OffsetIndex() OffsetIndex { return indexedOffsetIndex{col} }

func (col *indexedColumnBuffer) BloomFilter() BloomFilter { return nil }

func (col *indexedColumnBuffer) Dictionary() Dictionary { return col.dict }

func (col *indexedColumnBuffer) Pages() Pages { return onePage(col.Page()) }

func (col *indexedColumnBuffer) Page() BufferedPage { return &col.indexedPage }

func (col *indexedColumnBuffer) Reset() { col.values = col.values[:0] }

func (col *indexedColumnBuffer) Cap() int { return cap(col.values) }

func (col *indexedColumnBuffer) Len() int { return len(col.values) }

func (col *indexedColumnBuffer) Less(i, j int) bool {
	u := col.dict.Index(int(col.values[i]))
	v := col.dict.Index(int(col.values[j]))
	return col.typ.Compare(u, v) < 0
}

func (col *indexedColumnBuffer) Swap(i, j int) {
	col.values[i], col.values[j] = col.values[j], col.values[i]
}

func (col *indexedColumnBuffer) WriteValues(values []Value) (int, error) {
	for _, v := range values {
		col.values = append(col.values, int32(col.dict.Insert(v)))
	}
	return len(values), nil
}

func (col *indexedColumnBuffer) WriteRow(row Row) error {
	if len(row) == 0 {
		return errRowHasTooFewValues(int64(len(row)))
	}
	if len(row) > 1 {
		return errRowHasTooManyValues(int64(len(row)))
	}
	col.values = append(col.values, int32(col.dict.Insert(row[0])))
	return nil
}

func (col *indexedColumnBuffer) ReadRowAt(row Row, index int64) (Row, error) {
	switch {
	case index < 0:
		return row, errRowIndexOutOfBounds(index, int64(len(col.values)))
	case index >= int64(len(col.values)):
		return row, io.EOF
	default:
		v := col.dict.Index(int(col.values[index]))
		v.columnIndex = col.columnIndex
		return append(row, v), nil
	}
}

type indexedColumnReader struct {
	dict        Dictionary
	typ         Type
	decoder     encoding.Decoder
	buffer      []int32
	offset      int
	columnIndex int16
}

func newIndexedColumnReader(dict Dictionary, typ Type, columnIndex int16, bufferSize int) *indexedColumnReader {
	return &indexedColumnReader{
		dict:        dict,
		typ:         typ,
		buffer:      make([]int32, 0, atLeastOne(bufferSize)),
		columnIndex: ^columnIndex,
	}
}

func (r *indexedColumnReader) Type() Type { return r.typ }

func (r *indexedColumnReader) Column() int { return int(^r.columnIndex) }

func (r *indexedColumnReader) ReadValues(values []Value) (int, error) {
	i := 0
	for {
		for r.offset < len(r.buffer) && i < len(values) {
			count := len(r.buffer) - r.offset
			limit := len(values) - i

			if count > limit {
				count = limit
			}

			indexes := r.buffer[r.offset : r.offset+count]
			dictLen := r.dict.Len()
			for _, index := range indexes {
				if index < 0 || int(index) >= dictLen {
					return i, fmt.Errorf("reading value from indexed page: index out of bounds: %d/%d", index, dictLen)
				}
			}

			r.dict.Lookup(indexes, values[i:])
			r.offset += count

			j := i
			i += int(count)
			for j < i {
				values[j].columnIndex = r.columnIndex
				j++
			}
		}

		if i == len(values) {
			return i, nil
		}

		buffer := r.buffer[:cap(r.buffer)]
		n, err := r.decoder.DecodeInt32(buffer)
		if n == 0 {
			return i, err
		}

		r.buffer = buffer[:n]
		r.offset = 0
	}
}

func (r *indexedColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer = r.buffer[:0]
	r.offset = 0
}

type indexedColumnIndex struct{ col *indexedColumnBuffer }

func (index indexedColumnIndex) NumPages() int       { return 1 }
func (index indexedColumnIndex) NullCount(int) int64 { return 0 }
func (index indexedColumnIndex) NullPage(int) bool   { return false }
func (index indexedColumnIndex) MinValue(int) []byte {
	min, _ := index.col.Bounds()
	return min.Bytes()
}
func (index indexedColumnIndex) MaxValue(int) []byte {
	_, max := index.col.Bounds()
	return max.Bytes()
}
func (index indexedColumnIndex) IsAscending() bool {
	return index.col.typ.Compare(index.col.Bounds()) < 0
}
func (index indexedColumnIndex) IsDescending() bool {
	return index.col.typ.Compare(index.col.Bounds()) > 0
}

type indexedOffsetIndex struct{ col *indexedColumnBuffer }

func (index indexedOffsetIndex) NumPages() int                { return 1 }
func (index indexedOffsetIndex) Offset(int) int64             { return 0 }
func (index indexedOffsetIndex) CompressedPageSize(int) int64 { return index.col.Size() }
func (index indexedOffsetIndex) FirstRowIndex(int) int64      { return 0 }
