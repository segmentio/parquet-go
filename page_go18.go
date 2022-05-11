//go:build go1.18

package parquet

import (
	"io"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/internal/unsafecast"
)

type page[T primitive] struct {
	class       *class[T]
	values      []T
	columnIndex int16
}

func newPage[T primitive](columnIndex int16, numValues int32, data []byte, class *class[T]) *page[T] {
	var values = unsafecast.Slice[T](data)
	var zero T
	for len(values) < int(numValues) {
		values = append(values, zero)
	}
	if len(values) > int(numValues) {
		values = values[:numValues]
	}
	return &page[T]{
		class:       class,
		values:      values,
		columnIndex: ^columnIndex,
	}
}

func (p *page[T]) Column() int { return int(^p.columnIndex) }

func (p *page[T]) Dictionary() Dictionary { return nil }

func (p *page[T]) NumRows() int64 { return int64(len(p.values)) }

func (p *page[T]) NumValues() int64 { return int64(len(p.values)) }

func (p *page[T]) NumNulls() int64 { return 0 }

func (p *page[T]) min() T { return p.class.min(p.values) }

func (p *page[T]) max() T { return p.class.max(p.values) }

func (p *page[T]) bounds() (T, T) { return p.class.bounds(p.values) }

func (p *page[T]) Bounds() (min, max Value, ok bool) {
	if ok = len(p.values) > 0; ok {
		minValue, maxValue := p.bounds()
		min = p.class.makeValue(minValue)
		max = p.class.makeValue(maxValue)
	}
	return min, max, ok
}

func (p *page[T]) Clone() BufferedPage {
	return &page[T]{
		class:       p.class,
		values:      append([]T{}, p.values...),
		columnIndex: p.columnIndex,
	}
}

func (p *page[T]) Slice(i, j int64) BufferedPage {
	return &page[T]{
		class:       p.class,
		values:      p.values[i:j],
		columnIndex: p.columnIndex,
	}
}

func (p *page[T]) Size() int64 { return int64(len(p.values)) * int64(sizeof[T]()) }

func (p *page[T]) RepetitionLevels() []int8 { return nil }

func (p *page[T]) DefinitionLevels() []int8 { return nil }

func (p *page[T]) Values() ValueReader { return &pageValueReader[T]{page: p} }

func (p *page[T]) Buffer() BufferedPage { return p }

func (p *page[T]) Encode(dst []byte, enc encoding.Encoding) ([]byte, error) {
	return p.class.encode(enc, dst, p.values)
}

type pageValueReader[T primitive] struct {
	page   *page[T]
	offset int
}

func (r *pageValueReader[T]) Read(b []byte) (n int, err error) {
	n, err = r.ReadRequired(unsafecast.BytesToSlice[T](b))
	return sizeof[T]() * n, err
}

func (r *pageValueReader[T]) ReadRequired(values []T) (n int, err error) {
	n = copy(values, r.page.values[r.offset:])
	r.offset += n
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

func (r *pageValueReader[T]) ReadValues(values []Value) (n int, err error) {
	makeValue := r.page.class.makeValue
	pageValues := r.page.values
	columnIndex := r.page.columnIndex
	for n < len(values) && r.offset < len(pageValues) {
		values[n] = makeValue(pageValues[r.offset])
		values[n].columnIndex = columnIndex
		r.offset++
		n++
	}
	if r.offset == len(pageValues) {
		err = io.EOF
	}
	return n, err
}

var (
	_ RequiredReader[bool] = (*pageValueReader[bool])(nil)
)
