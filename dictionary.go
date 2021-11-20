package parquet

import (
	"encoding/binary"
	"math"

	"github.com/google/uuid"
	"github.com/segmentio/parquet/encoding"
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

	// Return the array of values indexed by this dictionary.
	Values() encoding.IntArray

	// Returns the number of key indexed in the dictionary.
	Len() int

	// Returns the dictionary key at the given index.
	Index(int) Value

	// Resets the dictionary to its initial state, removing all keys and values.
	Reset()

	// Dictionary implements the ValueWriter interface. Programs add values to a
	// dictionary by calling the WriteValue method.
	//
	// Dictionaries have a capacity limit defined by their buffer size. When the
	// limit is reached because too many keys have been written to a dictionary,
	// the WriteValue call returns ErrBufferFull.
	ValueWriter
}

type booleanDictionary struct {
	typ    Type
	keys   [8]byte
	values encoding.IntArray
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
		values: encoding.NewFixedIntArray(1),
	}
}

func (d *booleanDictionary) Type() Type { return d.typ }

func (d *booleanDictionary) Keys() []byte { return d.keys[:] }

func (d *booleanDictionary) Values() encoding.IntArray { return d.values }

func (d *booleanDictionary) Len() int { return 2 }

func (d *booleanDictionary) Index(i int) Value { return makeValueBoolean(d.keys[5+(2*i)] != 0) }

func (d *booleanDictionary) Reset() { d.values.Reset() }

func (d *booleanDictionary) WriteValue(v Value) error {
	key := int64(0)
	if v.Boolean() {
		key = 1
	}
	d.values.Append(key)
	return nil
}

type int32Dictionary struct {
	typ    Type
	keys   []int32
	index  map[int32]int32
	values encoding.IntArray
}

func newInt32Dictionary(typ Type, bufferSize int) *int32Dictionary {
	bufferSize /= 2
	return &int32Dictionary{
		typ:    typ,
		keys:   make([]int32, 0, bufferSize/4),
		index:  make(map[int32]int32, bufferSize/4),
		values: encoding.NewIntArray(),
	}
}

func (d *int32Dictionary) Type() Type { return d.typ }

func (d *int32Dictionary) Keys() []byte { return bits.Int32ToBytes(d.keys) }

func (d *int32Dictionary) Values() encoding.IntArray { return d.values }

func (d *int32Dictionary) Len() int { return len(d.keys) }

func (d *int32Dictionary) Index(i int) Value { return makeValueInt32(d.keys[i]) }

func (d *int32Dictionary) Reset() {
	d.keys = d.keys[:0]
	d.values.Reset()

	for key := range d.index {
		delete(d.index, key)
	}
}

func (d *int32Dictionary) WriteValue(v Value) error {
	return d.writeValue(v.Int32())
}

func (d *int32Dictionary) writeValue(key int32) error {
	if index, exists := d.index[key]; exists {
		d.values.Append(int64(index))
	} else {
		if len(d.keys) == cap(d.keys) {
			return ErrBufferFull
		}
		d.index[key] = int32(len(d.keys))
		d.keys = append(d.keys, key)
	}
	return nil
}

type int64Dictionary struct {
	typ    Type
	keys   []int64
	index  map[int64]int32
	values encoding.IntArray
}

func newInt64Dictionary(typ Type, bufferSize int) *int64Dictionary {
	bufferSize /= 2
	return &int64Dictionary{
		typ:    typ,
		keys:   make([]int64, 0, bufferSize/8),
		index:  make(map[int64]int32, bufferSize/8),
		values: encoding.NewIntArray(),
	}
}

func (d *int64Dictionary) Type() Type { return d.typ }

func (d *int64Dictionary) Keys() []byte { return bits.Int64ToBytes(d.keys) }

func (d *int64Dictionary) Values() encoding.IntArray { return d.values }

func (d *int64Dictionary) Len() int { return len(d.keys) }

func (d *int64Dictionary) Index(i int) Value { return makeValueInt64(d.keys[i]) }

func (d *int64Dictionary) Reset() {
	d.keys = d.keys[:0]
	d.values.Reset()

	for key := range d.index {
		delete(d.index, key)
	}
}

func (d *int64Dictionary) WriteValue(v Value) error {
	return d.writeValue(v.Int64())
}

func (d *int64Dictionary) writeValue(key int64) error {
	if index, exists := d.index[key]; exists {
		d.values.Append(int64(index))
	} else {
		if len(d.keys) == cap(d.keys) {
			return ErrBufferFull
		}
		d.index[key] = int32(len(d.keys))
		d.keys = append(d.keys, key)
	}
	return nil
}

type int96Dictionary struct{ *fixedLenByteArrayDictionary }

func newInt96Dictionary(typ Type, bufferSize int) int96Dictionary {
	return int96Dictionary{newFixedLenByteArrayDictionary(typ, bufferSize)}
}

func (d int96Dictionary) Index(i int) Value {
	return makeValueInt96(*(*[12]byte)(d.key(i)))
}

type floatDictionary struct{ *int32Dictionary }

func newFloatDictionary(typ Type, bufferSize int) floatDictionary {
	return floatDictionary{newInt32Dictionary(typ, bufferSize)}
}

func (d floatDictionary) Index(i int) Value {
	return makeValueFloat(math.Float32frombits(uint32(d.keys[i])))
}

func (d floatDictionary) WriteValue(v Value) error {
	return d.writeValue(int32(math.Float32bits(v.Float())))
}

type doubleDictionary struct{ *int64Dictionary }

func newDoubleDictionary(typ Type, bufferSize int) doubleDictionary {
	return doubleDictionary{newInt64Dictionary(typ, bufferSize)}
}

func (d doubleDictionary) Index(i int) Value {
	return makeValueDouble(math.Float64frombits(uint64(d.keys[i])))
}

func (d doubleDictionary) WriteValue(v Value) error {
	return d.writeValue(int64(math.Float64bits(v.Double())))
}

type byteArrayDictionary struct {
	typ    Type
	keys   []byte
	offset []uint32
	index  map[string]int32
	values encoding.IntArray
}

func newByteArrayDictionary(typ Type, bufferSize int) *byteArrayDictionary {
	return &byteArrayDictionary{
		typ:    typ,
		keys:   make([]byte, 0, bufferSize/4),
		offset: make([]uint32, 0, bufferSize/(4*4)),
		index:  make(map[string]int32, bufferSize/4),
		values: encoding.NewIntArray(),
	}
}

func (d *byteArrayDictionary) Type() Type { return d.typ }

func (d *byteArrayDictionary) Keys() []byte { return d.keys }

func (d *byteArrayDictionary) Values() encoding.IntArray { return d.values }

func (d *byteArrayDictionary) Len() int { return len(d.offset) }

func (d *byteArrayDictionary) Index(i int) Value {
	offset := d.offset[i]
	length := binary.LittleEndian.Uint32(d.keys[offset:])
	return makeValueBytes(ByteArray, d.keys[offset+4:offset+length+4])
}

func (d *byteArrayDictionary) Reset() {
	d.keys = d.keys[:0]
	d.offset = d.offset[:0]
	d.values.Reset()

	for key := range d.index {
		delete(d.index, key)
	}
}

func (d *byteArrayDictionary) WriteValue(v Value) error {
	k := v.ByteArray()

	if index, exists := d.index[string(k)]; exists {
		d.values.Append(int64(index))
	} else {
		if len(d.offset) == cap(d.offset) {
			return ErrBufferFull
		}

		i := len(d.keys)
		n := make([]byte, 4)
		binary.LittleEndian.PutUint32(n, uint32(len(k)))
		d.keys = append(d.keys, n...)
		d.keys = append(d.keys, k...)

		key := bits.BytesToString(d.keys[i+4:])
		d.index[key] = int32(len(d.offset))
		d.values.Append(int64(len(d.offset)))
		d.offset = append(d.offset, uint32(i))
	}

	return nil
}

type fixedLenByteArrayDictionary struct {
	typ    Type
	size   int
	keys   []byte
	index  map[string]int32
	values encoding.IntArray
}

func newFixedLenByteArrayDictionary(typ Type, bufferSize int) *fixedLenByteArrayDictionary {
	size := typ.Length()
	if typ.Kind() != FixedLenByteArray {
		size = bits.ByteCount(uint(size))
	}
	bufferSize /= 2
	bufferSize = ((bufferSize / size) + 1) * size
	return &fixedLenByteArrayDictionary{
		typ:    typ,
		size:   size,
		keys:   make([]byte, 0, bufferSize),
		index:  make(map[string]int32, bufferSize/size),
		values: encoding.NewIntArray(),
	}
}

func (d *fixedLenByteArrayDictionary) Type() Type { return d.typ }

func (d *fixedLenByteArrayDictionary) Keys() []byte { return d.keys }

func (d *fixedLenByteArrayDictionary) Values() encoding.IntArray { return d.values }

func (d *fixedLenByteArrayDictionary) Len() int { return len(d.keys) / d.size }

func (d *fixedLenByteArrayDictionary) Index(i int) Value {
	return makeValueBytes(FixedLenByteArray, d.key(i))
}

func (d *fixedLenByteArrayDictionary) key(i int) []byte {
	return d.keys[i*d.size : (i+1)*d.size]
}

func (d *fixedLenByteArrayDictionary) Reset() {
	d.keys = d.keys[:0]
	d.values.Reset()

	for key := range d.index {
		delete(d.index, key)
	}
}

func (d *fixedLenByteArrayDictionary) WriteValue(v Value) error {
	return d.writeValue(v.ByteArray())
}

func (d *fixedLenByteArrayDictionary) writeValue(key []byte) error {
	if index, exists := d.index[string(key)]; exists {
		d.values.Append(int64(index))
	} else {
		if len(d.keys) == cap(d.keys) {
			return ErrBufferFull
		}
		i := d.Len()
		n := len(d.keys)
		d.keys = append(d.keys, key...)
		d.index[bits.BytesToString(d.keys[n:])] = int32(i)
		d.values.Append(int64(i))
	}
	return nil
}

type uuidDictionary struct{ *fixedLenByteArrayDictionary }

func (d uuidDictionary) WriteValue(v Value) error {
	b := v.ByteArray()
	if len(b) != 16 {
		u, err := uuid.ParseBytes(b)
		if err != nil {
			return err
		}
		b = u[:]
	}
	return d.writeValue(b)
}

func NewDictionaryPageBuffer(dict Dictionary) PageBuffer { return &dictionaryPageBuffer{dict: dict} }

type dictionaryPageBuffer struct{ dict Dictionary }

func (buf *dictionaryPageBuffer) Type() Type { return buf.dict.Type() }

func (buf *dictionaryPageBuffer) Reset() { buf.dict.Values().Reset() }

func (buf *dictionaryPageBuffer) NumValues() int { return buf.dict.Values().Len() }

func (buf *dictionaryPageBuffer) DistinctCount() int { return buf.dict.Len() }

func (buf *dictionaryPageBuffer) Bounds() (min, max Value) {
	values := buf.dict.Values()

	if n := values.Len(); n > 0 {
		min = buf.dict.Index(int(values.Index(0)))
		max = buf.dict.Index(int(values.Index(0)))
		t := buf.dict.Type()

		for i := 1; i < n; i++ {
			v := buf.dict.Index(int(values.Index(i)))
			if t.Less(v, min) {
				min = v
			}
			if t.Less(max, v) {
				max = v
			}
		}

		min = min.Clone()
		max = max.Clone()
	}

	return min, max
}

func (buf *dictionaryPageBuffer) WriteValue(value Value) error {
	return buf.dict.WriteValue(value) // TODO: handle ErrBufferFull
}

func (buf *dictionaryPageBuffer) WriteTo(enc encoding.Encoder) error {
	return enc.EncodeIntArray(buf.dict.Values())
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
