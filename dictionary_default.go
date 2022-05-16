//go:build !go1.18

package parquet

import (
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/internal/bits"
)

// The boolean dictionary always contains two values for true and false.
type booleanDictionary struct {
	booleanPage
	typ   Type
	index map[bool]int32
}

func newBooleanDictionary(typ Type, columnIndex int16, numValues int32, data []byte) *booleanDictionary {
	return &booleanDictionary{
		typ: typ,
		booleanPage: booleanPage{
			values:      bits.BytesToBool(data),
			columnIndex: ^columnIndex,
		},
	}
}

func (d *booleanDictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *booleanDictionary) Len() int { return 2 }

func (d *booleanDictionary) Index(i int32) Value { return makeValueBoolean(d.values[i]) }

func (d *booleanDictionary) Insert(indexes []int32, values []Value) {
	_ = indexes[:len(values)]

	if d.index == nil {
		d.index = make(map[bool]int32, cap(d.values))
		for i, v := range d.values {
			d.index[v] = int32(i)
		}
	}

	for i, v := range values {
		value := v.Boolean()

		index, exists := d.index[value]
		if !exists {
			index = int32(len(d.values))
			d.values = append(d.values, value)
			d.index[value] = index
		}

		indexes[i] = index
	}
}

func (d *booleanDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(j)
	}
}

func (d *booleanDictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.values[indexes[0]]
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := d.values[i]
			switch {
			case compareBool(value, minValue) < 0:
				minValue = value
			case compareBool(value, maxValue) > 0:
				maxValue = value
			}
		}

		min = makeValueBoolean(minValue)
		max = makeValueBoolean(maxValue)
	}
	return min, max
}

func (d *booleanDictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

func (d *booleanDictionary) Page() BufferedPage {
	return &d.booleanPage
}

type int32Dictionary struct {
	int32Page
	typ   Type
	index map[int32]int32
}

func newInt32Dictionary(typ Type, columnIndex int16, numValues int32, data []byte) *int32Dictionary {
	return &int32Dictionary{
		typ: typ,
		int32Page: int32Page{
			values:      bits.BytesToInt32(data),
			columnIndex: ^columnIndex,
		},
	}
}

func (d *int32Dictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *int32Dictionary) Len() int { return len(d.values) }

func (d *int32Dictionary) Index(i int32) Value { return makeValueInt32(d.values[i]) }

func (d *int32Dictionary) Insert(indexes []int32, values []Value) {
	_ = indexes[:len(values)]

	if d.index == nil {
		d.index = make(map[int32]int32, cap(d.values))
		for i, v := range d.values {
			d.index[v] = int32(i)
		}
	}

	for i, v := range values {
		value := v.Int32()

		index, exists := d.index[value]
		if !exists {
			index = int32(len(d.values))
			d.values = append(d.values, value)
			d.index[value] = index
		}

		indexes[i] = index
	}
}

func (d *int32Dictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(j)
	}
}

func (d *int32Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.values[indexes[0]]
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := d.values[i]
			switch {
			case value < minValue:
				minValue = value
			case value > maxValue:
				maxValue = value
			}
		}

		min = makeValueInt32(minValue)
		max = makeValueInt32(maxValue)
	}
	return min, max
}

func (d *int32Dictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

func (d *int32Dictionary) Page() BufferedPage {
	return &d.int32Page
}

type int64Dictionary struct {
	int64Page
	typ   Type
	index map[int64]int32
}

func newInt64Dictionary(typ Type, columnIndex int16, numValues int32, data []byte) *int64Dictionary {
	return &int64Dictionary{
		typ: typ,
		int64Page: int64Page{
			values:      bits.BytesToInt64(data),
			columnIndex: ^columnIndex,
		},
	}
}

func (d *int64Dictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *int64Dictionary) Len() int { return len(d.values) }

func (d *int64Dictionary) Index(i int32) Value { return makeValueInt64(d.values[i]) }

func (d *int64Dictionary) Insert(indexes []int32, values []Value) {
	_ = indexes[:len(values)]

	if d.index == nil {
		d.index = make(map[int64]int32, cap(d.values))
		for i, v := range d.values {
			d.index[v] = int32(i)
		}
	}

	for i, v := range values {
		value := v.Int64()

		index, exists := d.index[value]
		if !exists {
			index = int32(len(d.values))
			d.values = append(d.values, value)
			d.index[value] = index
		}

		indexes[i] = index
	}
}

func (d *int64Dictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(j)
	}
}

func (d *int64Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.values[indexes[0]]
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := d.values[i]
			switch {
			case value < minValue:
				minValue = value
			case value > maxValue:
				maxValue = value
			}
		}

		min = makeValueInt64(minValue)
		max = makeValueInt64(maxValue)
	}
	return min, max
}

func (d *int64Dictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

func (d *int64Dictionary) Page() BufferedPage {
	return &d.int64Page
}

type int96Dictionary struct {
	int96Page
	typ   Type
	index map[deprecated.Int96]int32
}

func newInt96Dictionary(typ Type, columnIndex int16, numValues int32, data []byte) *int96Dictionary {
	return &int96Dictionary{
		typ: typ,
		int96Page: int96Page{
			values:      deprecated.BytesToInt96(data),
			columnIndex: ^columnIndex,
		},
	}
}

func (d *int96Dictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *int96Dictionary) Len() int { return len(d.values) }

func (d *int96Dictionary) Index(i int32) Value { return makeValueInt96(d.values[i]) }

func (d *int96Dictionary) Insert(indexes []int32, values []Value) {
	_ = indexes[:len(values)]

	if d.index == nil {
		d.index = make(map[deprecated.Int96]int32, cap(d.values))
		for i, v := range d.values {
			d.index[v] = int32(i)
		}
	}

	for i, v := range values {
		value := v.Int96()

		index, exists := d.index[value]
		if !exists {
			index = int32(len(d.values))
			d.values = append(d.values, value)
			d.index[value] = index
		}

		indexes[i] = index
	}
}

func (d *int96Dictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(j)
	}
}

func (d *int96Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.values[indexes[0]]
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := d.values[i]
			switch {
			case value.Less(minValue):
				minValue = value
			case maxValue.Less(value):
				maxValue = value
			}
		}

		min = makeValueInt96(minValue)
		max = makeValueInt96(maxValue)
	}
	return min, max
}

func (d *int96Dictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

func (d *int96Dictionary) Page() BufferedPage {
	return &d.int96Page
}

type floatDictionary struct {
	floatPage
	typ   Type
	index map[float32]int32
}

func newFloatDictionary(typ Type, columnIndex int16, numValues int32, data []byte) *floatDictionary {
	return &floatDictionary{
		typ: typ,
		floatPage: floatPage{
			values:      bits.BytesToFloat32(data),
			columnIndex: ^columnIndex,
		},
	}
}

func (d *floatDictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *floatDictionary) Len() int { return len(d.values) }

func (d *floatDictionary) Index(i int32) Value { return makeValueFloat(d.values[i]) }

func (d *floatDictionary) Insert(indexes []int32, values []Value) {
	_ = indexes[:len(values)]

	if d.index == nil {
		d.index = make(map[float32]int32, cap(d.values))
		for i, v := range d.values {
			d.index[v] = int32(i)
		}
	}

	for i, v := range values {
		value := v.Float()

		index, exists := d.index[value]
		if !exists {
			index = int32(len(d.values))
			d.values = append(d.values, value)
			d.index[value] = index
		}

		indexes[i] = index
	}
}

func (d *floatDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(j)
	}
}

func (d *floatDictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.values[indexes[0]]
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := d.values[i]
			switch {
			case value < minValue:
				minValue = value
			case value > maxValue:
				maxValue = value
			}
		}

		min = makeValueFloat(minValue)
		max = makeValueFloat(maxValue)
	}
	return min, max
}

func (d *floatDictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

func (d *floatDictionary) Page() BufferedPage {
	return &d.floatPage
}

type doubleDictionary struct {
	doublePage
	typ   Type
	index map[float64]int32
}

func newDoubleDictionary(typ Type, columnIndex int16, numValues int32, data []byte) *doubleDictionary {
	return &doubleDictionary{
		typ: typ,
		doublePage: doublePage{
			values:      bits.BytesToFloat64(data),
			columnIndex: ^columnIndex,
		},
	}
}

func (d *doubleDictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *doubleDictionary) Len() int { return len(d.values) }

func (d *doubleDictionary) Index(i int32) Value { return makeValueDouble(d.values[i]) }

func (d *doubleDictionary) Insert(indexes []int32, values []Value) {
	_ = indexes[:len(values)]

	if d.index == nil {
		d.index = make(map[float64]int32, cap(d.values))
		for i, v := range d.values {
			d.index[v] = int32(i)
		}
	}

	for i, v := range values {
		value := v.Double()

		index, exists := d.index[value]
		if !exists {
			index = int32(len(d.values))
			d.values = append(d.values, value)
			d.index[value] = index
		}

		indexes[i] = index
	}
}

func (d *doubleDictionary) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(j)
	}
}

func (d *doubleDictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.values[indexes[0]]
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := d.values[i]
			switch {
			case value < minValue:
				minValue = value
			case value > maxValue:
				maxValue = value
			}
		}

		min = makeValueDouble(minValue)
		max = makeValueDouble(maxValue)
	}
	return min, max
}

func (d *doubleDictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

func (d *doubleDictionary) Page() BufferedPage {
	return &d.doublePage
}

type uint32Dictionary struct{ *int32Dictionary }

func newUint32Dictionary(typ Type, columnIndex int16, numValues int32, data []byte) uint32Dictionary {
	return uint32Dictionary{newInt32Dictionary(typ, columnIndex, numValues, data)}
}

func (d uint32Dictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d uint32Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := uint32(d.values[indexes[0]])
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := uint32(d.values[i])
			switch {
			case value < minValue:
				minValue = value
			case value > maxValue:
				maxValue = value
			}
		}

		min = makeValueInt32(int32(minValue))
		max = makeValueInt32(int32(maxValue))
	}
	return min, max
}

func (d uint32Dictionary) Page() BufferedPage {
	return uint32Page{&d.int32Page}
}

type uint64Dictionary struct{ *int64Dictionary }

func newUint64Dictionary(typ Type, columnIndex int16, numValues int32, data []byte) uint64Dictionary {
	return uint64Dictionary{newInt64Dictionary(typ, columnIndex, numValues, data)}
}

func (d uint64Dictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d uint64Dictionary) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := uint64(d.values[indexes[0]])
		maxValue := minValue

		for _, i := range indexes[1:] {
			value := uint64(d.values[i])
			switch {
			case value < minValue:
				minValue = value
			case value > maxValue:
				maxValue = value
			}
		}

		min = makeValueInt64(int64(minValue))
		max = makeValueInt64(int64(maxValue))
	}
	return min, max
}

func (d uint64Dictionary) Page() BufferedPage {
	return uint64Page{&d.int64Page}
}
