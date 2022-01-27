//go:build !purego

package xxhash

// This file contains the declaration of signatures for the multi hashing
// functions implemented in sum64uint_amd64.s, which provides vectorized
// versions of the algorithms written in sum64uint_purego.go.
//
// The use of SIMD optimization yields measurable throughput increases when
// computing multiple hash values in parallel compared to hashing values
// individually in loops:
//
// name                   old time/op    new time/op    delta
// MultiSum64Uint64/16B     5.97ns ± 1%    6.01ns ± 3%     ~     (p=0.841 n=5+5)
// MultiSum64Uint64/100B    27.1ns ± 6%    22.9ns ± 1%  -15.38%  (p=0.008 n=5+5)
// MultiSum64Uint64/4KB     1.05µs ± 1%    0.79µs ± 3%  -25.33%  (p=0.008 n=5+5)
// MultiSum64Uint64/10MB    2.78ms ± 1%    2.23ms ± 3%  -19.79%  (p=0.008 n=5+5)
//
// name                   old speed      new speed      delta
// MultiSum64Uint64/16B   2.68GB/s ± 1%  2.66GB/s ± 3%     ~     (p=0.841 n=5+5)
// MultiSum64Uint64/100B  3.69GB/s ± 6%  4.36GB/s ± 1%  +18.06%  (p=0.008 n=5+5)
// MultiSum64Uint64/4KB   3.80GB/s ± 1%  5.09GB/s ± 3%  +33.95%  (p=0.008 n=5+5)
// MultiSum64Uint64/10MB  3.59GB/s ± 1%  4.48GB/s ± 3%  +24.72%  (p=0.008 n=5+5)

//go:noescape
func MultiSum64Uint8(h []uint64, v []uint8) int

//go:noescape
func MultiSum64Uint16(h []uint64, v []uint16) int

//go:noescape
func MultiSum64Uint32(h []uint64, v []uint32) int

//go:noescape
func MultiSum64Uint64(h []uint64, v []uint64) int
