package bloom_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/filter/bloom"
)

func TestFilter(t *testing.T) {
	const N = 1000
	const S = 3
	f := make(bloom.Filter, bloom.NumBlocksOf(N, 10))
	p := rand.New(rand.NewSource(S))

	for i := 0; i < N; i++ {
		f.Insert(p.Uint64())
	}

	type filter interface {
		Check(uint64) bool
	}

	for _, test := range []struct {
		scenario string
		filter   filter
	}{
		{scenario: "filter", filter: f},
		{scenario: "reader", filter: newSerializedFilter(f.Bytes())},
	} {
		t.Run(test.scenario, func(t *testing.T) {
			p.Seed(S)
			falsePositives := 0

			for i := 0; i < N; i++ {
				x := p.Uint64()

				if !test.filter.Check(x) {
					t.Fatalf("bloom filter block does not contain the value that was inserted: %d", x)
				}
				if test.filter.Check(^x) {
					falsePositives++
				}
			}

			if r := (float64(falsePositives) / N); r > 0.01 {
				t.Fatalf("bloom filter triggered too many false positives: %g%%", r*100)
			}
		})
	}
}

type serializedFilter struct {
	bytes.Reader
	buf bloom.Block
}

func (f *serializedFilter) Check(x uint64) bool {
	ok, _ := bloom.Check(&f.Reader, f.Size(), &f.buf, x)
	return ok
}

func newSerializedFilter(b []byte) *serializedFilter {
	f := new(serializedFilter)
	f.Reset(b)
	return f
}

func BenchmarkFilterInsert(b *testing.B) {
	f := make(bloom.Filter, 1)
	for i := 0; i < b.N; i++ {
		f.Insert(uint64(i))
	}
	b.SetBytes(bloom.BlockSize)
}

func BenchmarkFilterCheck(b *testing.B) {
	f := make(bloom.Filter, 1)
	f.Insert(42)
	for i := 0; i < b.N; i++ {
		f.Check(42)
	}
	b.SetBytes(bloom.BlockSize)
}
