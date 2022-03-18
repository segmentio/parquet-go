//go:build go1.18

package parquet

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet-go/encoding"
)

type dictionary[T primitive] struct {
	class  *class[T]
	typ    Type
	values []T
	index  map[T]int32
}

func newDictionary[T primitive](typ Type, bufferSize int, class *class[T]) *dictionary[T] {
	return &dictionary[T]{
		class:  class,
		typ:    typ,
		values: make([]T, 0, dictCap(bufferSize, sizeof[T]())),
	}
}

func (d *dictionary[T]) Type() Type { return newIndexedType(d.typ, d) }

func (d *dictionary[T]) Len() int { return len(d.values) }

func (d *dictionary[T]) Index(i int) Value { return d.class.makeValue(d.values[i]) }

func (d *dictionary[T]) Insert(v Value) int { return d.insert(d.class.value(v)) }

func (d *dictionary[T]) insert(value T) int {
	if index, exists := d.index[value]; exists {
		return int(index)
	}
	if d.index == nil {
		d.index = make(map[T]int32, cap(d.values))
		for i, v := range d.values {
			d.index[v] = int32(i)
		}
	}
	index := len(d.values)
	d.index[value] = int32(index)
	d.values = append(d.values, value)
	return index
}

func (d *dictionary[T]) Lookup(indexes []int32, values []Value) {
	for i, j := range indexes {
		values[i] = d.Index(int(j))
	}
}

func (d *dictionary[T]) Bounds(indexes []int32) (min, max Value) {
	if len(indexes) > 0 {
		minValue := d.values[indexes[0]]
		maxValue := minValue
		less := d.class.less

		for _, i := range indexes[1:] {
			value := d.values[i]
			switch {
			case less(value, minValue):
				minValue = value
			case less(maxValue, value):
				maxValue = value
			}
		}

		makeValue := d.class.makeValue
		min = makeValue(minValue)
		max = makeValue(maxValue)
	}
	return min, max
}

func (d *dictionary[T]) ReadFrom(decoder encoding.Decoder) error {
	d.Reset()
	for {
		if len(d.values) == cap(d.values) {
			newValues := make([]T, len(d.values), 2*cap(d.values))
			copy(newValues, d.values)
			d.values = newValues
		}

		n, err := d.class.decode(decoder, d.values[len(d.values):cap(d.values)])
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

func (d *dictionary[T]) WriteTo(encoder encoding.Encoder) error {
	if err := d.class.encode(encoder, d.values); err != nil {
		return fmt.Errorf("writing parquet dictionary of %d int64 values: %w", d.Len(), err)
	}
	return nil
}

func (d *dictionary[T]) Reset() {
	d.values = d.values[:0]
	d.index = nil
}
