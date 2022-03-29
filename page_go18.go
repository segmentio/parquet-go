//go:build go1.18

package parquet

import (
	"github.com/segmentio/parquet-go/encoding"
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

func (p *page[T]) Bounds() (min, max Value) {
	if len(p.values) > 0 {
		minValue, maxValue := p.bounds()
		min = p.class.makeValue(minValue)
		max = p.class.makeValue(maxValue)
	}
	return min, max
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

func (p *page[T]) Buffer() BufferedPage { return p }

func (p *page[T]) Values() ValueReader {
	return &valueReader[T]{
		class:       p.class,
		values:      p.values,
		columnIndex: p.columnIndex,
	}
}
