package parquet

import (
	"math"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/internal/bits"
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
	Keys() []byte

	// Returns the number of key indexed in the dictionary.
	Len() int

	// Returns the dictionary key at the given index.
	Index(int) Value

	// Inserts a value to the dictionary, returning the index at which it was
	// recorded.
	Insert(Value) (int, error)

	// Resets the dictionary to its initial state, removing all keys and values.
	Reset()
}

type booleanDictionary struct {
	typ  Type
	keys [8]byte
}

func newBooleanDictionary(typ Type) *booleanDictionary {
	return &booleanDictionary{
		typ: typ,
		keys: [8]byte{
			// rle-bit-packed-hybrid: <length> <encoded-data>
			0: 4,
			1: 0,
			2: 0,
			3: 0,
			// rle-run := <rle-header> <repeated-value>
			// rle-header := varint-encode( (rle-run-len) << 1)
			4: 1 << 1,
			5: 0,
			6: 1 << 1,
			7: 1,
		},
	}
}

func (d *booleanDictionary) Type() Type { return d.typ }

func (d *booleanDictionary) Keys() []byte { return d.keys[:] }

func (d *booleanDictionary) Len() int { return 2 }

func (d *booleanDictionary) Index(i int) Value { return makeValueBoolean(d.keys[5+(2*i)] != 0) }

func (d *booleanDictionary) Insert(v Value) (int, error) {
	if v.Boolean() {
		return 1, nil
	} else {
		return 0, nil
	}
}

func (d *booleanDictionary) Reset() {}

type int32Dictionary struct {
	typ   Type
	keys  []int32
	index map[int32]int32
}

func newInt32Dictionary(typ Type, bufferSize int) *int32Dictionary {
	bufferSize /= 2
	return &int32Dictionary{
		typ:   typ,
		keys:  make([]int32, 0, bufferSize/4),
		index: make(map[int32]int32, bufferSize/4),
	}
}

func (d *int32Dictionary) Type() Type { return d.typ }

func (d *int32Dictionary) Keys() []byte { return bits.Int32ToBytes(d.keys) }

func (d *int32Dictionary) Len() int { return len(d.keys) }

func (d *int32Dictionary) Index(i int) Value { return makeValueInt32(d.keys[i]) }

func (d *int32Dictionary) Insert(v Value) (int, error) { return d.insert(v.Int32()) }

func (d *int32Dictionary) insert(key int32) (int, error) {
	if index, exists := d.index[key]; exists {
		return int(index), nil
	}
	index := len(d.keys)
	d.index[key] = int32(index)
	d.keys = append(d.keys, key)
	return index, nil
}

func (d *int32Dictionary) Reset() {
	d.keys = d.keys[:0]

	for key := range d.index {
		delete(d.index, key)
	}
}

type int64Dictionary struct {
	typ   Type
	keys  []int64
	index map[int64]int32
}

func newInt64Dictionary(typ Type, bufferSize int) *int64Dictionary {
	bufferSize /= 2
	return &int64Dictionary{
		typ:   typ,
		keys:  make([]int64, 0, bufferSize/8),
		index: make(map[int64]int32, bufferSize/8),
	}
}

func (d *int64Dictionary) Type() Type { return d.typ }

func (d *int64Dictionary) Keys() []byte { return bits.Int64ToBytes(d.keys) }

func (d *int64Dictionary) Len() int { return len(d.keys) }

func (d *int64Dictionary) Index(i int) Value { return makeValueInt64(d.keys[i]) }

func (d *int64Dictionary) Insert(v Value) (int, error) { return d.insert(v.Int64()) }

func (d *int64Dictionary) insert(key int64) (int, error) {
	if index, exists := d.index[key]; exists {
		return int(index), nil
	}
	index := len(d.keys)
	d.index[key] = int32(index)
	d.keys = append(d.keys, key)
	return index, nil
}

func (d *int64Dictionary) Reset() {
	d.keys = d.keys[:0]

	for key := range d.index {
		delete(d.index, key)
	}
}

type int96Dictionary struct{ *fixedLenByteArrayDictionary }

func newInt96Dictionary(typ Type, bufferSize int) int96Dictionary {
	return int96Dictionary{newFixedLenByteArrayDictionary(typ, bufferSize)}
}

func (d int96Dictionary) Index(i int) Value {
	return makeValueInt96(*(*int96)(d.key(i)))
}

type floatDictionary struct{ *int32Dictionary }

func newFloatDictionary(typ Type, bufferSize int) floatDictionary {
	return floatDictionary{newInt32Dictionary(typ, bufferSize)}
}

func (d floatDictionary) Index(i int) Value {
	return makeValueFloat(math.Float32frombits(uint32(d.keys[i])))
}

func (d floatDictionary) Insert(v Value) (int, error) {
	return d.insert(int32(math.Float32bits(v.Float())))
}

type doubleDictionary struct{ *int64Dictionary }

func newDoubleDictionary(typ Type, bufferSize int) doubleDictionary {
	return doubleDictionary{newInt64Dictionary(typ, bufferSize)}
}

func (d doubleDictionary) Index(i int) Value {
	return makeValueDouble(math.Float64frombits(uint64(d.keys[i])))
}

func (d doubleDictionary) Insert(v Value) (int, error) {
	return d.insert(int64(math.Float64bits(v.Double())))
}

type byteArrayDictionary struct {
	typ    Type
	keys   []byte
	offset []int32
	index  map[string]int32
}

func newByteArrayDictionary(typ Type, bufferSize int) *byteArrayDictionary {
	return &byteArrayDictionary{
		typ:    typ,
		keys:   make([]byte, 0, bufferSize/4),
		offset: make([]int32, 0, bufferSize/(4*4)),
		index:  make(map[string]int32, bufferSize/4),
	}
}

func (d *byteArrayDictionary) Type() Type { return d.typ }

func (d *byteArrayDictionary) Keys() []byte { return d.keys }

func (d *byteArrayDictionary) Len() int { return len(d.offset) }

func (d *byteArrayDictionary) Index(i int) Value {
	offset := d.offset[i]
	value, _ := plain.SplitByteArray(d.keys[offset:])
	return makeValueBytes(ByteArray, value)
}

func (d *byteArrayDictionary) Insert(v Value) (int, error) {
	return d.insert(v.ByteArray())
}

func (d *byteArrayDictionary) insert(key []byte) (int, error) {
	if index, exists := d.index[string(key)]; exists {
		return int(index), nil
	}

	offset := len(d.keys)
	d.keys = plain.AppendByteArray(d.keys, key)
	stringKey := bits.BytesToString(d.keys[offset+4:])

	index := len(d.offset)
	d.index[stringKey] = int32(index)
	d.offset = append(d.offset, int32(offset))
	return index, nil
}

func (d *byteArrayDictionary) Reset() {
	d.keys = d.keys[:0]
	d.offset = d.offset[:0]

	for key := range d.index {
		delete(d.index, key)
	}
}

type fixedLenByteArrayDictionary struct {
	typ   Type
	size  int
	keys  []byte
	index map[string]int32
}

func newFixedLenByteArrayDictionary(typ Type, bufferSize int) *fixedLenByteArrayDictionary {
	size := typ.Length()
	if typ.Kind() != FixedLenByteArray {
		size = bits.ByteCount(uint(size))
	}
	bufferSize /= 2
	bufferSize = ((bufferSize / size) + 1) * size
	return &fixedLenByteArrayDictionary{
		typ:   typ,
		size:  size,
		keys:  make([]byte, 0, bufferSize),
		index: make(map[string]int32, bufferSize/size),
	}
}

func (d *fixedLenByteArrayDictionary) Type() Type { return d.typ }

func (d *fixedLenByteArrayDictionary) Keys() []byte { return d.keys }

func (d *fixedLenByteArrayDictionary) Len() int { return len(d.keys) / d.size }

func (d *fixedLenByteArrayDictionary) Index(i int) Value {
	return makeValueBytes(FixedLenByteArray, d.key(i))
}

func (d *fixedLenByteArrayDictionary) key(i int) []byte {
	return d.keys[i*d.size : (i+1)*d.size]
}

func (d *fixedLenByteArrayDictionary) Insert(v Value) (int, error) {
	return d.insert(v.ByteArray())
}

func (d *fixedLenByteArrayDictionary) insert(key []byte) (int, error) {
	if index, exists := d.index[string(key)]; exists {
		return int(index), nil
	}
	i := d.Len()
	n := len(d.keys)
	d.keys = append(d.keys, key...)
	d.index[bits.BytesToString(d.keys[n:])] = int32(i)
	return i, nil
}

func (d *fixedLenByteArrayDictionary) Reset() {
	d.keys = d.keys[:0]

	for key := range d.index {
		delete(d.index, key)
	}
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

func NewIndexedPageWriter(encoder encoding.Encoder, bufferSize int, dict Dictionary) PageWriter {
	return &indexedPageWriter{
		dict:    dict,
		encoder: encoder,
		values:  make([]int32, 0, bufferSize/4),
	}
}

type indexedPageWriter struct {
	dict    Dictionary
	encoder encoding.Encoder
	values  []int32
	min     Value
	max     Value
	count   int
}

func (w *indexedPageWriter) Type() Type { return w.dict.Type() }

func (w *indexedPageWriter) NumValues() int { return w.count }

func (w *indexedPageWriter) DistinctCount() int { return 0 }

func (w *indexedPageWriter) Bounds() (min, max Value) {
	if w.count > 0 {
		min = w.min
		max = w.max
	}
	return min, max
}

func (w *indexedPageWriter) WriteValue(value Value) error {
	i, err := w.dict.Insert(value)
	if err != nil {
		return err
	}

	if w.count == 0 {
		w.min = value
		w.max = value
	} else {
		typ := w.Type()
		if typ.Less(value, w.min) {
			w.min = value
		}
		if typ.Less(w.max, value) {
			w.max = value
		}
	}

	w.values = append(w.values, int32(i))
	w.count++
	return nil
}

func (w *indexedPageWriter) Flush() error {
	defer func() { w.values = w.values[:0] }()
	return w.encoder.EncodeInt32(w.values)
}

func (w *indexedPageWriter) Reset(encoder encoding.Encoder) {
	w.encoder = encoder
	w.values = w.values[:0]
	w.min = Value{}
	w.max = Value{}
	w.count = 0
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
