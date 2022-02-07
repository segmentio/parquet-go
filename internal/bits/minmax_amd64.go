//go:build !purego

package bits

// The max-max algorithms combine looking for the min and max values in a single
// pass over the data. While the behavior is the same as calling functions to
// look for the min and max values independently, doing both operations at the
// same time means that we only load the data from memory once. When working on
// large arrays the algorithms are limited by memory bandwidth, computing both
// the min and max together shrinks by half the amount of data read from memory.
//
// The following benchmarks results were highlighitng the benefits of combining
// the min-max search, compared to calling the min and max functions separately:
//
// name                 old time/op    new time/op    delta
// MinMaxInt64/10240KB     590µs ±15%     330µs ±10%  -44.01%  (p=0.000 n=10+10)
//
// name                 old speed      new speed      delta
// MinMaxInt64/10240KB  17.9GB/s ±13%  31.8GB/s ±11%  +78.13%  (p=0.000 n=10+10)
//
// As expected, since the functions are memory-bound in those cases, and load
// half as much data, we significant improvements. The gains are not 2x because
// running more AVX-512 instructions in the tight loops causes more contention
// on CPU ports.
//
//
// As all things are always trade offs, using min/max functions independently
// actually yields better throughput when the data resides in CPU caches:
//
// name             old time/op    new time/op    delta
// MinMaxInt64/4KB    46.2ns ± 1%    52.1ns ± 0%  +12.65%  (p=0.000 n=10+10)
//
// name             old speed      new speed      delta
// MinMaxInt64/4KB  88.6GB/s ± 1%  78.6GB/s ± 0%  -11.23%  (p=0.000 n=10+10)
//
// The probable explanation is that in those cases the algorithms are not
// memory-bound anymore, but limited by contention on CPU ports, and the
// inidividual min/max functions are able to better parallelize the work due
// to running less instructions per loop. The performance starts to equalize
// around 256KiB, and degrade beyond 1MiB, so we use this threshold to determine
// which approach to prefer.
const combinedMinMaxThreshold = 1 * 1024 * 1024

//go:noescape
func combinedMinMaxBool(data []bool) (min, max bool)

//go:noescape
func combinedMinMaxInt32(data []int32) (min, max int32)

//go:noescape
func combinedMinMaxInt64(data []int64) (min, max int64)

//go:noescape
func combinedMinMaxUint32(data []uint32) (min, max uint32)

//go:noescape
func combinedMinMaxUint64(data []uint64) (min, max uint64)

//go:noescape
func combinedMinMaxFloat32(data []float32) (min, max float32)

//go:noescape
func combinedMinMaxFloat64(data []float64) (min, max float64)

//go:noescape
func combinedMinMaxBE128(data []byte) (min, max []byte)

func minMaxBool(data []bool) (min, max bool) {
	if 1*len(data) >= combinedMinMaxThreshold {
		return combinedMinMaxBool(data)
	}
	min = minBool(data)
	max = maxBool(data)
	return
}

func minMaxInt32(data []int32) (min, max int32) {
	if 4*len(data) >= combinedMinMaxThreshold {
		return combinedMinMaxInt32(data)
	}
	min = minInt32(data)
	max = maxInt32(data)
	return
}

func minMaxInt64(data []int64) (min, max int64) {
	if 8*len(data) >= combinedMinMaxThreshold {
		return combinedMinMaxInt64(data)
	}
	min = minInt64(data)
	max = maxInt64(data)
	return
}

func minMaxUint32(data []uint32) (min, max uint32) {
	if 4*len(data) >= combinedMinMaxThreshold {
		return combinedMinMaxUint32(data)
	}
	min = minUint32(data)
	max = maxUint32(data)
	return
}

func minMaxUint64(data []uint64) (min, max uint64) {
	if 8*len(data) >= combinedMinMaxThreshold {
		return combinedMinMaxUint64(data)
	}
	min = minUint64(data)
	max = maxUint64(data)
	return
}

func minMaxFloat32(data []float32) (min, max float32) {
	if 4*len(data) >= combinedMinMaxThreshold {
		return combinedMinMaxFloat32(data)
	}
	min = minFloat32(data)
	max = maxFloat32(data)
	return
}

func minMaxFloat64(data []float64) (min, max float64) {
	if 8*len(data) >= combinedMinMaxThreshold {
		return combinedMinMaxFloat64(data)
	}
	min = minFloat64(data)
	max = maxFloat64(data)
	return
}

func minMaxBE128(data []byte) (min, max []byte) {
	// TODO: min/max BE128 is really complex to vectorize, and the returns
	// were barely better than doing the min and max independently, for all
	// input sizes. We should revisit if we find ways to improve the min or
	// max algorithms which can be transposed to the combined version.
	min = minBE128(data)
	max = maxBE128(data)
	return
}
