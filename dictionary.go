package parquet

import (
	"fmt"
	"io"
	"math"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
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
	Insert(Value) (int, error)

	// Given an array of dictionary indexes, lookup the values into the array
	// of values passed as second argument.
	//
	// The method panics if len(indexes) > len(values), or one of the indexes
	// is negative or greater than the highest index in the dictionary.
	Lookup(indexes []int32, values []Value)

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

// The boolean dictionary always contains two values for true and false.
type booleanDictionary struct {
	typ    Type
	values [2]bool
}

func newBooleanDictionary(typ Type) *booleanDictionary {
	return &booleanDictionary{
		typ:    typ,
		values: [2]bool{false, true},
	}
}

func (d *booleanDictionary) Type() Type { return d.typ }

func (d *booleanDictionary) Len() int { return 2 }

func (d *booleanDictionary) Index(i int) Value { return makeValueBoolean(d.values[i]) }

func (d *booleanDictionary) Insert(v Value) (int, error) {
	if v.Boolean() {
		return 1, nil
	} else {
		return 0, nil
	}
}

func (d *booleanDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

func (d *booleanDictionary) ReadFrom(decoder encoding.Decoder) error {
	_, err := decoder.DecodeBoolean(d.values[:])
	d.Reset()
	if err != nil {
		if err == io.EOF {
			err = nil
		} else {
			err = fmt.Errorf("reading parquet dictionary of boolean values: %w", err)
		}
	}
	return err
}

func (d *booleanDictionary) WriteTo(encoder encoding.Encoder) error {
	if err := encoder.EncodeBoolean(d.values[:]); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d boolean values: %w", d.Len(), err)
	}
	return nil
}

func (d *booleanDictionary) Reset() {
	d.values = [2]bool{false, true}
}

type int32Dictionary struct {
	typ    Type
	values []int32
	index  map[int32]int32
}

func newInt32Dictionary(typ Type, bufferSize int) *int32Dictionary {
	const valueItemSize = 4
	const indexItemSize = 4 + 4 + mapSizeOverheadPerItem
	capacity := bufferSize / (valueItemSize + indexItemSize)
	return &int32Dictionary{
		typ:    typ,
		values: make([]int32, 0, capacity),
	}
}

func (d *int32Dictionary) Type() Type { return d.typ }

func (d *int32Dictionary) Len() int { return len(d.values) }

func (d *int32Dictionary) Index(i int) Value { return makeValueInt32(d.values[i]) }

func (d *int32Dictionary) Insert(v Value) (int, error) { return d.insert(v.Int32()) }

func (d *int32Dictionary) insert(value int32) (int, error) {
	if index, exists := d.index[value]; exists {
		return int(index), nil
	}
	if d.index == nil {
		d.index = make(map[int32]int32, cap(d.values))
	}
	index := len(d.values)
	d.index[value] = int32(index)
	d.values = append(d.values, value)
	return index, nil
}

func (d *int32Dictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

func (d *int32Dictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()

	if cap(d.values) == 0 {
		d.values = make([]int32, 0, defaultBufferSize/4)
	}

	for {
		if len(d.values) == cap(d.values) {
			newValues := make([]int32, len(d.values), 2*cap(d.values))
			copy(newValues, d.values)
			d.values = newValues
		}

		n, err := decoder.DecodeInt32(d.values[len(d.values):cap(d.values)])
		if n > 0 {
			d.values = d.values[:len(d.values)+n]
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("reading parquet dictionary of int32 values: %w", err)
			}
			return err
		}
	}
}

func (d *int32Dictionary) WriteTo(encoder encoding.Encoder) error {
	if err := encoder.EncodeInt32(d.values); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d int32 values: %w", d.Len(), err)
	}
	return nil
}

func (d *int32Dictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

type int64Dictionary struct {
	typ    Type
	values []int64
	index  map[int64]int32
}

func newInt64Dictionary(typ Type, bufferSize int) *int64Dictionary {
	const valueItemSize = 8
	const indexItemSize = 8 + 4 + mapSizeOverheadPerItem
	capacity := bufferSize / (valueItemSize + indexItemSize)
	return &int64Dictionary{
		typ:    typ,
		values: make([]int64, 0, capacity),
	}
}

func (d *int64Dictionary) Type() Type { return d.typ }

func (d *int64Dictionary) Len() int { return len(d.values) }

func (d *int64Dictionary) Index(i int) Value { return makeValueInt64(d.values[i]) }

func (d *int64Dictionary) Insert(v Value) (int, error) { return d.insert(v.Int64()) }

func (d *int64Dictionary) insert(value int64) (int, error) {
	if index, exists := d.index[value]; exists {
		return int(index), nil
	}
	if d.index == nil {
		d.index = make(map[int64]int32, cap(d.values))
	}
	index := len(d.values)
	d.index[value] = int32(index)
	d.values = append(d.values, value)
	return index, nil
}

func (d *int64Dictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

func (d *int64Dictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()

	if cap(d.values) == 0 {
		d.values = make([]int64, 0, defaultBufferSize/8)
	}

	for {
		if len(d.values) == cap(d.values) {
			newValues := make([]int64, len(d.values), 2*cap(d.values))
			copy(newValues, d.values)
			d.values = newValues
		}

		n, err := decoder.DecodeInt64(d.values[len(d.values):cap(d.values)])
		if n > 0 {
			d.values = d.values[:len(d.values)+n]
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("reading parquet dictionary of int64 values: %w", err)
			}
			return err
		}
	}
}

func (d *int64Dictionary) WriteTo(encoder encoding.Encoder) error {
	if err := encoder.EncodeInt64(d.values); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d int64 values: %w", d.Len(), err)
	}
	return nil
}

func (d *int64Dictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

type int96Dictionary struct {
	typ    Type
	values []deprecated.Int96
	index  map[deprecated.Int96]int32
}

func newInt96Dictionary(typ Type, bufferSize int) *int96Dictionary {
	const valueItemSize = 12
	const indexItemSize = 12 + 4 + mapSizeOverheadPerItem
	capacity := bufferSize / (valueItemSize + indexItemSize)
	return &int96Dictionary{
		typ:    typ,
		values: make([]deprecated.Int96, 0, capacity),
	}
}

func (d *int96Dictionary) Type() Type { return d.typ }

func (d *int96Dictionary) Len() int { return len(d.values) }

func (d *int96Dictionary) Index(i int) Value { return makeValueInt96(d.values[i]) }

func (d *int96Dictionary) Insert(v Value) (int, error) { return d.insert(v.Int96()) }

func (d *int96Dictionary) insert(value deprecated.Int96) (int, error) {
	if index, exists := d.index[value]; exists {
		return int(index), nil
	}
	if d.index == nil {
		d.index = make(map[deprecated.Int96]int32, cap(d.values))
	}
	index := len(d.values)
	d.index[value] = int32(index)
	d.values = append(d.values, value)
	return index, nil
}

func (d *int96Dictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

func (d *int96Dictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()

	if cap(d.values) == 0 {
		d.values = make([]deprecated.Int96, 0, defaultBufferSize/12)
	}

	for {
		if len(d.values) == cap(d.values) {
			newValues := make([]deprecated.Int96, len(d.values), 2*cap(d.values))
			copy(newValues, d.values)
			d.values = newValues
		}

		n, err := decoder.DecodeInt96(d.values[len(d.values):cap(d.values)])
		if n > 0 {
			d.values = d.values[:len(d.values)+n]
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("reading parquet dictionary of int96 values: %w", err)
			}
			return err
		}
	}
}

func (d *int96Dictionary) WriteTo(encoder encoding.Encoder) error {
	if err := encoder.EncodeInt96(d.values); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d int96 values: %w", d.Len(), err)
	}
	return nil
}

func (d *int96Dictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

type floatDictionary struct{ *int32Dictionary }

func newFloatDictionary(typ Type, bufferSize int) floatDictionary {
	return floatDictionary{newInt32Dictionary(typ, bufferSize)}
}

func (d floatDictionary) Index(i int) Value {
	return makeValueFloat(math.Float32frombits(uint32(d.values[i])))
}

func (d floatDictionary) Insert(v Value) (int, error) {
	return d.insert(int32(math.Float32bits(v.Float())))
}

func (d floatDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

type doubleDictionary struct{ *int64Dictionary }

func newDoubleDictionary(typ Type, bufferSize int) doubleDictionary {
	return doubleDictionary{newInt64Dictionary(typ, bufferSize)}
}

func (d doubleDictionary) Index(i int) Value {
	return makeValueDouble(math.Float64frombits(uint64(d.values[i])))
}

func (d doubleDictionary) Insert(v Value) (int, error) {
	return d.insert(int64(math.Float64bits(v.Double())))
}

func (d doubleDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

type byteArrayDictionary struct {
	typ    Type
	values encoding.ByteArrayList
	index  map[string]int32
}

func newByteArrayDictionary(typ Type, bufferSize int) *byteArrayDictionary {
	capacity := bufferSize / 16
	return &byteArrayDictionary{
		typ:    typ,
		values: encoding.MakeByteArrayList(atLeastOne(capacity)),
	}
}

func (d *byteArrayDictionary) Type() Type { return d.typ }

func (d *byteArrayDictionary) Len() int { return d.values.Len() }

func (d *byteArrayDictionary) Index(i int) Value { return makeValueBytes(ByteArray, d.values.Index(i)) }

func (d *byteArrayDictionary) Insert(v Value) (int, error) { return d.insert(v.ByteArray()) }

func (d *byteArrayDictionary) insert(value []byte) (int, error) {
	if index, exists := d.index[string(value)]; exists {
		return int(index), nil
	}

	d.values.Push(value)
	index := d.values.Len() - 1
	stringValue := bits.BytesToString(d.values.Index(index))

	if d.index == nil {
		d.index = make(map[string]int32, d.values.Cap())
	}
	d.index[stringValue] = int32(index)
	return index, nil
}

func (d *byteArrayDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
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
	const indexItemSize = 16 + 4 + mapSizeOverheadPerItem
	size := typeSizeOf(typ)
	capacity := bufferSize / (size + indexItemSize)
	return &fixedLenByteArrayDictionary{
		typ:    typ,
		size:   size,
		values: make([]byte, 0, capacity*size),
	}
}

func (d *fixedLenByteArrayDictionary) Type() Type { return d.typ }

func (d *fixedLenByteArrayDictionary) Len() int { return len(d.values) / d.size }

func (d *fixedLenByteArrayDictionary) Index(i int) Value {
	return makeValueBytes(FixedLenByteArray, d.value(i))
}

func (d *fixedLenByteArrayDictionary) value(i int) []byte {
	return d.values[i*d.size : (i+1)*d.size]
}

func (d *fixedLenByteArrayDictionary) Insert(v Value) (int, error) {
	return d.insert(v.ByteArray())
}

func (d *fixedLenByteArrayDictionary) insert(value []byte) (int, error) {
	if index, exists := d.index[string(value)]; exists {
		return int(index), nil
	}
	if d.index == nil {
		d.index = make(map[string]int32, cap(d.values)/d.size)
	}
	i := d.Len()
	n := len(d.values)
	d.values = append(d.values, value...)
	d.index[bits.BytesToString(d.values[n:])] = int32(i)
	return i, nil
}

func (d *fixedLenByteArrayDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

func (d *fixedLenByteArrayDictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()

	if cap(d.values) == 0 {
		d.values = make([]byte, 0, ((defaultBufferSize/d.size)+1)*d.size)
	}

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

func NewIndexedPageReader(dict Dictionary, decoder encoding.Decoder, bufferSize int) PageReader {
	return &indexedPageReader{
		typ:     dict.Type(),
		dict:    dict,
		decoder: decoder,
		values:  make([]int32, 0, atLeastOne(bufferSize/4)),
	}
}

type indexedPageReader struct {
	typ     Type
	dict    Dictionary
	decoder encoding.Decoder
	values  []int32
	offset  uint
	batch   [1]Value
}

func (r *indexedPageReader) Type() Type { return r.typ }

func (r *indexedPageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *indexedPageReader) ReadValue() (Value, error) {
	_, err := r.ReadValueBatch(r.batch[:])
	v := r.batch[0]
	r.batch[0] = Value{}
	return v, err
}

func (r *indexedPageReader) ReadValueBatch(values []Value) (int, error) {
	i := 0
	for {
		for r.offset < uint(len(r.values)) && i < len(values) {
			count := uint(len(r.values)) - r.offset
			limit := uint(len(values) - i)

			if count > limit {
				count = limit
			}

			indexes := r.values[r.offset : r.offset+count]
			dictLen := r.dict.Len()
			for _, index := range indexes {
				if index < 0 || int(index) >= dictLen {
					return i, fmt.Errorf("reading value from indexed page: index out of bounds: %d/%d", index, dictLen)
				}
			}

			r.dict.Lookup(indexes, values[i:])
			r.offset += count
			i += int(count)
		}

		if i == len(values) {
			return i, nil
		}

		n, err := r.decoder.DecodeInt32(r.values[:cap(r.values)])
		if n == 0 {
			return i, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

func NewIndexedPageWriter(dict Dictionary, bufferSize int) PageWriter {
	return &indexedPageWriter{
		indexedPage: indexedPage{
			dict:   dict,
			values: make([]int32, 0, atLeastOne(bufferSize/4)),
		},
	}
}

type indexedPageWriter struct{ indexedPage }

func (w *indexedPageWriter) Page() Page { return &w.indexedPage }

func (w *indexedPageWriter) Reset() { w.values = w.values[:0] }

func (w *indexedPageWriter) WriteValue(value Value) error {
	i, err := w.dict.Insert(value)
	if err != nil {
		return err
	}
	w.values = append(w.values, int32(i))
	return nil
}

func (w *indexedPageWriter) WriteValueBatch(values []Value) (n int, err error) {
	for _, value := range values {
		if err := w.WriteValue(value); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

type indexedPage struct {
	dict   Dictionary
	values []int32
}

func newIndexedPage(dict Dictionary, values []int32) *indexedPage {
	return &indexedPage{dict: dict, values: values}
}

func (page *indexedPage) Type() Type { return page.dict.Type() }

func (page *indexedPage) NumValues() int { return len(page.values) }

func (page *indexedPage) NumNulls() int { return 0 }

func (page *indexedPage) Bounds() (min, max Value) {
	if len(page.values) > 0 {
		min = page.dict.Index(int(page.values[0]))
		max = min
		typ := page.Type()

		for _, i := range page.values[1:] {
			value := page.dict.Index(int(i))
			switch {
			case typ.Less(value, min):
				min = value
			case typ.Less(max, value):
				max = value
			}
		}
	}
	return min, max
}

func (page *indexedPage) Slice(i, j int) Page {
	return newIndexedPage(page.dict, page.values[i:j])
}

func (page *indexedPage) RepetitionLevels() []int8 { return nil }

func (page *indexedPage) DefinitionLevels() []int8 { return nil }

func (page *indexedPage) WriteTo(enc encoding.Encoder) error { return enc.EncodeInt32(page.values) }

var (
	_ Dictionary = (*booleanDictionary)(nil)
	_ Dictionary = (*int32Dictionary)(nil)
	_ Dictionary = (*int64Dictionary)(nil)
	_ Dictionary = (*int96Dictionary)(nil)
	_ Dictionary = (*floatDictionary)(nil)
	_ Dictionary = (*doubleDictionary)(nil)
	_ Dictionary = (*byteArrayDictionary)(nil)
	_ Dictionary = (*fixedLenByteArrayDictionary)(nil)
	_ PageReader = (*indexedPageReader)(nil)
	_ PageWriter = (*indexedPageWriter)(nil)
	_ Page       = (*indexedPage)(nil)
)
