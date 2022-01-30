//go:build !purego

package bloom

import "golang.org/x/sys/cpu"

// This file contains the signatures for bloom filter algorithms implemented in
// filter_amd64.s.
//
// The assembly code provides significant speedups on filter inserts and checks,
// with the greatest gains seen on the bulk insert operation where the use of
// vectorized code yields great results.
//
// The following sections record the kind of performance improvements we were
// able to measure, comparing with performing the filter block lookups in Go
// and calling to the block insert and check routines:
//
// name              old time/op    new time/op     delta
// FilterInsertBulk    45.1ns ± 2%    17.8ns ± 3%   -60.41%  (p=0.000 n=10+10)
// FilterInsert        3.48ns ± 2%     2.55ns ± 1%  -26.86%  (p=0.000 n=10+8)
// FilterCheck         3.64ns ± 3%     2.66ns ± 2%  -26.82%  (p=0.000 n=10+9)
//
// name              old speed      new speed       delta
// FilterInsertBulk  11.4GB/s ± 2%  28.7GB/s ± 3%  +152.61%  (p=0.000 n=10+10)
// FilterInsert      9.19GB/s ± 2%  12.56GB/s ± 1%  +36.71%  (p=0.000 n=10+8)
// FilterCheck       8.80GB/s ± 3%  12.03GB/s ± 2%  +36.61%  (p=0.000 n=10+9)

func filterInsertBulk(f []Block, x []uint64) {
	switch {
	case cpu.X86.HasAVX2:
		filterInsertBulkAVX2(f, x)
	default:
		filterInsertBulkGeneric(f, x)
	}
}

func filterInsert(f []Block, x uint64) {
	switch {
	case cpu.X86.HasAVX2:
		filterInsertAVX2(f, x)
	default:
		filterInsertGeneric(f, x)
	}
}

func filterCheck(f []Block, x uint64) bool {
	switch {
	case cpu.X86.HasAVX2:
		return filterCheckAVX2(f, x)
	default:
		return filterCheckGeneric(f, x)
	}
}

//go:noescape
func filterInsertBulkAVX2(f []Block, x []uint64)

//go:noescape
func filterInsertAVX2(f []Block, x uint64)

//go:noescape
func filterCheckAVX2(f []Block, x uint64) bool
