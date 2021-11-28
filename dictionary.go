package parquet

import (
	"fmt"
	"io"
	"math"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
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

	// Returns the dictionary keys (plain encoded).
	//
	// TODO: remove
	Keys() []byte

	// Returns the number of value indexed in the dictionary.
	Len() int

	// Returns the dictionary value at the given index.
	Index(int) Value

	// Inserts a value to the dictionary, returning the index at which it was
	// recorded.
	Insert(Value) (int, error)

	// Reads the dictionary from the decoder passed as argument.
	//
	// The dictionary is cleared prior to loading the values so that its final
	// content contains only the entries read from the decoder.
	ReadFrom(encoding.Decoder) error

	// Wrties the dictionary to the encoder passed as argument.
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

func (d *booleanDictionary) Keys() []byte { return nil }

func (d *booleanDictionary) Len() int { return 2 }

func (d *booleanDictionary) Index(i int) Value { return makeValueBoolean(d.values[i]) }

func (d *booleanDictionary) Insert(v Value) (int, error) {
	if v.Boolean() {
		return 1, nil
	} else {
		return 0, nil
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

func (d *int32Dictionary) Keys() []byte { return bits.Int32ToBytes(d.values) }

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

func (d *int64Dictionary) Keys() []byte { return bits.Int64ToBytes(d.values) }

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

type int96Dictionary struct{ *fixedLenByteArrayDictionary }

func newInt96Dictionary(typ Type, bufferSize int) int96Dictionary {
	return int96Dictionary{newFixedLenByteArrayDictionary(typ, bufferSize)}
}

func (d int96Dictionary) Index(i int) Value {
	return makeValueInt96(*(*int96)(d.value(i)))
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

type byteArrayDictionary struct {
	typ    Type
	values []byte
	offset []int32
	index  map[string]int32
}

func newByteArrayDictionary(typ Type, bufferSize int) *byteArrayDictionary {
	const valueItemSize = 20
	const offsetItemsize = 4
	const indexItemSize = 16 + 4 + mapSizeOverheadPerItem
	capacity := bufferSize / (valueItemSize + offsetItemsize + indexItemSize)
	return &byteArrayDictionary{
		typ:    typ,
		values: make([]byte, 0, capacity*valueItemSize),
		offset: make([]int32, 0, capacity),
	}
}

func (d *byteArrayDictionary) Type() Type { return d.typ }

func (d *byteArrayDictionary) Keys() []byte { return d.values }

func (d *byteArrayDictionary) Len() int { return len(d.offset) }

func (d *byteArrayDictionary) Index(i int) Value {
	offset := d.offset[i]
	value, _ := plain.SplitByteArray(d.values[offset:])
	return makeValueBytes(ByteArray, value)
}

func (d *byteArrayDictionary) Insert(v Value) (int, error) {
	return d.insert(v.ByteArray())
}

func (d *byteArrayDictionary) insert(value []byte) (int, error) {
	if index, exists := d.index[string(value)]; exists {
		return int(index), nil
	}

	offset := len(d.values)
	d.values = plain.AppendByteArray(d.values, value)
	stringValue := bits.BytesToString(d.values[offset+4:])

	if d.index == nil {
		d.index = make(map[string]int32, cap(d.offset))
	}
	index := len(d.offset)
	d.index[stringValue] = int32(index)
	d.offset = append(d.offset, int32(offset))
	return index, nil
}

func (d *byteArrayDictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()

	if cap(d.values) == 0 {
		d.values = make([]byte, 0, defaultBufferSize)
	}

	for {
		if (cap(d.values) - len(d.values)) < 4 {
			newValues := make([]byte, len(d.values), bits.NearestPowerOfTwo(len(d.values)+4))
			copy(newValues, d.values)
			d.values = newValues
		}

		buffer := d.values[len(d.values):cap(d.values)]
		n, err := decoder.DecodeByteArray(buffer)
		if n > 0 {
			offset := len(d.values)
			_, err := plain.ScanByteArrayList(buffer, n, func(value []byte) error {
				d.offset = append(d.offset, int32(offset))
				offset += 4 + len(value)
				return nil
			})
			d.values = d.values[:offset]
			if err != nil {
				return fmt.Errorf("reading parquet dictionary of binary values: %w", err)
			}
		}

		switch err {
		case nil:
		case io.EOF:
			return nil
		case encoding.ErrValueTooLarge:
			size := 4 + uint32(plain.ByteArrayLength(d.values[len(d.values):len(d.values)+4]))
			newValues := make([]byte, len(d.values), bits.NearestPowerOfTwo32(size))
			copy(newValues, d.values)
			d.values = newValues
		default:
			return fmt.Errorf("reading parquet dictionary of binary values: %w", err)
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
	d.values = d.values[:0]
	d.offset = d.offset[:0]
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

func (d *fixedLenByteArrayDictionary) Keys() []byte { return d.values }

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

type uuidDictionary struct{ *fixedLenByteArrayDictionary }

func (d uuidDictionary) Insert(v Value) (int, error) {
	b := v.ByteArray()
	if len(b) != 16 {
		u, err := uuid.ParseBytes(b)
		if err != nil {
			return -1, err
		}
		b = u[:]
	}
	return d.insert(b)
}

func NewDictionaryPageBuffer(dict Dictionary, bufferSize int) PageBuffer {
	return &dictionaryPageBuffer{
		dict:   dict,
		values: make([]int32, 0, bufferSize/4),
	}
}

type dictionaryPageBuffer struct {
	dict   Dictionary
	values []int32
}

func (buf *dictionaryPageBuffer) Type() Type { return buf.dict.Type() }

func (buf *dictionaryPageBuffer) Reset() { buf.values = buf.values[:0] }

func (buf *dictionaryPageBuffer) NumValues() int { return len(buf.values) }

func (buf *dictionaryPageBuffer) DistinctCount() int { return 0 }

func (buf *dictionaryPageBuffer) Bounds() (min, max Value) {
	if values := buf.values; len(values) > 0 {
		min = buf.dict.Index(int(values[0]))
		max = buf.dict.Index(int(values[0]))
		typ := buf.dict.Type()

		for _, index := range values {
			value := buf.dict.Index(int(index))
			if typ.Less(value, min) {
				min = value
			}
			if typ.Less(max, value) {
				max = value
			}
		}

		min = min.Clone()
		max = max.Clone()
	}
	return min, max
}

func (buf *dictionaryPageBuffer) WriteValue(value Value) error {
	i, err := buf.dict.Insert(value)
	if err != nil {
		return err
	}
	buf.values = append(buf.values, int32(i))
	return nil
}

func (buf *dictionaryPageBuffer) WriteTo(enc encoding.Encoder) error {
	return enc.EncodeInt32(buf.values)
}

func NewIndexedPageReader(decoder encoding.Decoder, bufferSize int, dict Dictionary) PageReader {
	return &indexedPageReader{
		dict:    dict,
		decoder: decoder,
		values:  make([]int32, 0, atLeastOne(bufferSize/4)),
	}
}

func NewIndexedPageWriter(encoder encoding.Encoder, bufferSize int, dict Dictionary) PageWriter {
	return &indexedPageWriter{
		dict:    dict,
		encoder: encoder,
		values:  make([]int32, 0, atLeastOne(bufferSize/4)),
	}
}

type indexedPageReader struct {
	dict    Dictionary
	decoder encoding.Decoder
	values  []int32
	offset  uint
}

func (r *indexedPageReader) Type() Type { return r.dict.Type() }

func (r *indexedPageReader) NumValues() int { return 0 }

func (r *indexedPageReader) NumNulls() int { return 0 }

func (r *indexedPageReader) DistinctCount() int { return 0 }

func (r *indexedPageReader) Bounds() (min, max Value) { return }

func (r *indexedPageReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.values = r.values[:0]
	r.offset = 0
}

func (r *indexedPageReader) ReadValue() (Value, error) {
	for {
		if r.offset < uint(len(r.values)) {
			index := int(r.values[r.offset])

			if index >= 0 && index < r.dict.Len() {
				r.offset++
				return r.dict.Index(index), nil
			}

			return Value{}, fmt.Errorf("reading value from indexed page: index out of bounds: %d/%d", index, r.dict.Len())
		}

		n, err := r.decoder.DecodeInt32(r.values[:cap(r.values)])
		if n == 0 {
			return Value{}, err
		}

		r.values = r.values[:n]
		r.offset = 0
	}
}

type indexedPageWriter struct {
	dict    Dictionary
	encoder encoding.Encoder
	values  []int32
	min     int
	max     int
	count   int
}

func (w *indexedPageWriter) Type() Type { return w.dict.Type() }

func (w *indexedPageWriter) NumValues() int { return w.count }

func (w *indexedPageWriter) DistinctCount() int { return 0 }

func (w *indexedPageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		minIndex, maxIndex := w.bounds()
		min = w.dict.Index(minIndex)
		max = w.dict.Index(maxIndex)
	}
	return min, max
}

func (w *indexedPageWriter) WriteValue(value Value) error {
	i, err := w.dict.Insert(value)
	if err != nil {
		return err
	}
	w.values = append(w.values, int32(i))
	w.count++
	return nil
}

func (w *indexedPageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	w.min, w.max = w.bounds()
	return w.encoder.EncodeInt32(w.values)
}

func (w *indexedPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = -1
	w.max = -1
	w.count = 0
}

func (w *indexedPageWriter) bounds() (min, max int) {
	min = w.min
	max = w.max

	if len(w.values) > 0 {
		if min < 0 {
			min = int(w.values[0])
		}
		if max < 0 {
			max = int(w.values[0])
		}

		typ := w.dict.Type()
		minValue := w.dict.Index(min)
		maxValue := w.dict.Index(max)

		for _, index := range w.values {
			value := w.dict.Index(int(index))

			if typ.Less(value, minValue) {
				min, minValue = int(index), value
			}
			if typ.Less(maxValue, value) {
				max, maxValue = int(index), value
			}
		}
	}

	return min, max
}

var (
	_ Dictionary = (*booleanDictionary)(nil)
	_ Dictionary = (*int32Dictionary)(nil)
	_ Dictionary = (*int64Dictionary)(nil)
	_ Dictionary = (*int96Dictionary)(nil)
	_ Dictionary = (*floatDictionary)(nil)
	_ Dictionary = (*doubleDictionary)(nil)
	_ Dictionary = (*byteArrayDictionary)(nil)
	_ Dictionary = (*fixedLenByteArrayDictionary)(nil)
)
