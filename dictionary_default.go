//go:build !go1.18

package parquet

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
)

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

func (d *booleanDictionary) Type() Type { return newIndexedType(d.typ, d) }

func (d *booleanDictionary) Len() int { return 2 }

func (d *booleanDictionary) Index(i int32) Value { return makeValueBoolean(d.values[i]) }

func (d *booleanDictionary) Insert(indexes []int32, values []Value) {
	_ = indexes[:len(values)]

	for i, v := range values {
		if v.Boolean() {
			indexes[i] = 1
		} else {
			indexes[i] = 0
		}
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
	return &int32Dictionary{
		typ:    typ,
		values: make([]int32, 0, dictCap(bufferSize, 4)),
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

func (d *int32Dictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()
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
	return &int64Dictionary{
		typ:    typ,
		values: make([]int64, 0, dictCap(bufferSize, 8)),
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

func (d *int64Dictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()
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
	return &int96Dictionary{
		typ:    typ,
		values: make([]deprecated.Int96, 0, dictCap(bufferSize, 12)),
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

func (d *int96Dictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()
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

type floatDictionary struct {
	typ    Type
	values []float32
	index  map[float32]int32
}

func newFloatDictionary(typ Type, bufferSize int) *floatDictionary {
	return &floatDictionary{
		typ:    typ,
		values: make([]float32, 0, dictCap(bufferSize, 4)),
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

func (d *floatDictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()
	for {
		if len(d.values) == cap(d.values) {
			newValues := make([]float32, len(d.values), 2*cap(d.values))
			copy(newValues, d.values)
			d.values = newValues
		}

		n, err := decoder.DecodeFloat(d.values[len(d.values):cap(d.values)])
		if n > 0 {
			d.values = d.values[:len(d.values)+n]
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("reading parquet dictionary of float values: %w", err)
			}
			return err
		}
	}
}

func (d *floatDictionary) WriteTo(encoder encoding.Encoder) error {
	if err := encoder.EncodeFloat(d.values); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d float values: %w", d.Len(), err)
	}
	return nil
}

func (d *floatDictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

type doubleDictionary struct {
	typ    Type
	values []float64
	index  map[float64]int32
}

func newDoubleDictionary(typ Type, bufferSize int) *doubleDictionary {
	return &doubleDictionary{
		typ:    typ,
		values: make([]float64, 0, dictCap(bufferSize, 8)),
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

func (d *doubleDictionary) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()
	for {
		if len(d.values) == cap(d.values) {
			newValues := make([]float64, len(d.values), 2*cap(d.values))
			copy(newValues, d.values)
			d.values = newValues
		}

		n, err := decoder.DecodeDouble(d.values[len(d.values):cap(d.values)])
		if n > 0 {
			d.values = d.values[:len(d.values)+n]
		}

		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("reading parquet dictionary of double values: %w", err)
			}
			return err
		}
	}
}

func (d *doubleDictionary) WriteTo(encoder encoding.Encoder) error {
	if err := encoder.EncodeDouble(d.values); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d double values: %w", d.Len(), err)
	}
	return nil
}

func (d *doubleDictionary) Reset() {
	d.values = d.values[:0]
	d.index = nil
}

type uint32Dictionary struct{ *int32Dictionary }

func newUint32Dictionary(typ Type, bufferSize int) uint32Dictionary {
	return uint32Dictionary{newInt32Dictionary(typ, bufferSize)}
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

type uint64Dictionary struct{ *int64Dictionary }

func newUint64Dictionary(typ Type, bufferSize int) uint64Dictionary {
	return uint64Dictionary{newInt64Dictionary(typ, bufferSize)}
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
