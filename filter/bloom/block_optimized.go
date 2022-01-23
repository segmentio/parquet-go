//go:build (!amd64 || purego) && !parquet.bloom.no_unroll

package bloom

// The functions in this file are optimized versions of the algorithms described
// in https://github.com/apache/parquet-format/blob/master/BloomFilter.md
//
// The functions are manual unrolling of the loops, which yield significant
// performance improvements:
//
// goos: darwin
// goarch: amd64
// pkg: github.com/segmentio/parquet-go/filter/bloom
// cpu: Intel(R) Core(TM) i9-8950HK CPU @ 2.90GHz
//
// name          old time/op  new time/op  delta
// BlockInsert   326ns ± 1%    11ns ± 4%  -96.72%  (p=0.000 n=8+9)
// BlockCheck    242ns ± 1%    11ns ± 4%  -95.61%  (p=0.000 n=9+9)

func mask(x uint32) Block {
	return Block{
		0: 1 << ((x * salt0) >> 27),
		1: 1 << ((x * salt1) >> 27),
		2: 1 << ((x * salt2) >> 27),
		3: 1 << ((x * salt3) >> 27),
		4: 1 << ((x * salt4) >> 27),
		5: 1 << ((x * salt5) >> 27),
		6: 1 << ((x * salt6) >> 27),
		7: 1 << ((x * salt7) >> 27),
	}
}

func (b *Block) Insert(x uint32) {
	masked := mask(x)
	b[0] |= masked[0]
	b[1] |= masked[1]
	b[2] |= masked[2]
	b[3] |= masked[3]
	b[4] |= masked[4]
	b[5] |= masked[5]
	b[6] |= masked[6]
	b[7] |= masked[7]
}

func (b *Block) Check(x uint32) bool {
	masked := mask(x)
	return ((b[0] & masked[0]) != 0) &&
		((b[1] & masked[1]) != 0) &&
		((b[2] & masked[2]) != 0) &&
		((b[3] & masked[3]) != 0) &&
		((b[4] & masked[4]) != 0) &&
		((b[5] & masked[5]) != 0) &&
		((b[6] & masked[6]) != 0) &&
		((b[7] & masked[7]) != 0)
}
