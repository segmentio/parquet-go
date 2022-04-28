//go:build go1.18

package parquet

import (
	"io"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/internal/cast"
)

type page[T primitive] struct {
	class       *class[T]
	values      []T
	columnIndex int16
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

func (p *page[T]) WriteTo(e encoding.Encoder) error { return p.class.encode(e, p.values) }

func (p *page[T]) Values() PageValues { return &pageValues[T]{page: p} }

func (p *page[T]) Buffer() BufferedPage { return p }

type pageValues[T primitive] struct {
	page   *page[T]
	offset int
}

func (r *pageValues[T]) Close() error {
	r.page = nil
	return nil
}

func (r *pageValues[T]) Read(b []byte) (n int, err error) {
	n, err = r.ReadRequired(cast.BytesToSlice[T](b))
	return sizeof[T]() * n, err
}

func (r *pageValues[T]) ReadRequired(values []T) (n int, err error) {
	if r.page == nil {
		return 0, io.EOF
	}
	n = copy(values, r.page.values[r.offset:])
	r.offset += n
	if r.offset == len(r.page.values) {
		err = io.EOF
	}
	return n, err
}

func (r *pageValues[T]) ReadValues(values []Value) (n int, err error) {
	if r.page == nil {
		return 0, io.EOF
	}

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

type pageDecoder[T primitive] struct {
	class       *class[T]
	buffer      []T
	typ         Type
	decoder     encoding.Decoder
	offset      int
	remain      int
	columnIndex int16
}

func newPageDecoder[T primitive](typ Type, columnIndex int16, numValues int, decoder encoding.Decoder, class *class[T]) *pageDecoder[T] {
	return &pageDecoder[T]{
		class:       class,
		buffer:      class.getBuffer(),
		typ:         typ,
		decoder:     decoder,
		remain:      numValues,
		columnIndex: ^columnIndex,
	}
}

func (r *pageDecoder[T]) Type() Type {
	return r.typ
}

func (r *pageDecoder[T]) Column() int {
	return int(^r.columnIndex)
}

func (r *pageDecoder[T]) Close() error {
	r.class.putBuffer(r.buffer)
	r.buffer = nil
	r.decoder = nil
	r.offset = 0
	r.remain = 0
	return nil
}

func (r *pageDecoder[T]) ReadValues(values []Value) (n int, err error) {
	if r.decoder == nil {
		return 0, io.EOF
	}

	makeValue := r.class.makeValue
	columnIndex := r.columnIndex

	for {
		for r.offset < len(r.buffer) && n < len(values) {
			values[n] = makeValue(r.buffer[r.offset])
			values[n].columnIndex = columnIndex
			r.offset++
			r.remain--
			n++
		}

		if r.remain == 0 || r.decoder == nil {
			return n, io.EOF
		}
		if n == len(values) {
			return n, nil
		}

		length := min(r.remain, cap(r.buffer))
		buffer := r.buffer[:length]
		d, err := r.class.decode(r.decoder, buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d]
		r.offset = 0
	}
}

var (
	_ RequiredReader[bool] = (*pageValues[bool])(nil)
)
