//go:build !purego

package bloom

// The functions in this file are SIMD-optimized versions of the functions
// declared in block_optimized.go for x86 targets.
//
// The optimization yields measurable improvements over the pure Go versions:
//
// goos: darwin
// goarch: amd64
// pkg: github.com/segmentio/parquet-go/filter/bloom
// cpu: Intel(R) Core(TM) i9-8950HK CPU @ 2.90GHz
//
// name         old time/op  new time/op  delta
// BlockInsert  10.7ns ± 4%   5.5ns ± 2%  -48.62%  (p=0.000 n=9+9)
// BlockCheck   10.6ns ± 4%   2.0ns ± 2%  -80.78%  (p=0.000 n=9+8)

//go:noescape
func block_insert(b *Block, x uint32)

//go:noescape
func block_check(b *Block, x uint32) bool

func (b *Block) Insert(x uint32) { block_insert(b, x) }

func (b *Block) Check(x uint32) bool { return block_check(b, x) }
