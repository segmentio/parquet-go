package bloom_test

import (
	"math"
	"testing"

	"github.com/segmentio/parquet-go/filter/bloom"
)

func TestBlock(t *testing.T) {
	for i := uint64(0); i < math.MaxUint32; i = (i * 2) + 1 {
		x := uint32(i)
		b := bloom.Block{}
		b.Insert(x)
		if !b.Check(x) {
			t.Fatalf("bloom filter block does not contain the value that was inserted: %d", x)
		}
		if b.Check(^x) {
			t.Fatalf("bloom filter block contains value that was not inserted: %d", ^x)
		}
	}
}

func BenchmarkBlockInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		x := bloom.Block{}
		x.Insert(uint32(i))
	}
}

func BenchmarkBlockCheck(b *testing.B) {
	x := bloom.Block{}
	x.Insert(42)
	for i := 0; i < b.N; i++ {
		x.Check(42)
	}
}
