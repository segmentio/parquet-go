//go:build !purego

package parquet

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

var (
	dictionaryBoundsInt32  = dictionaryBoundsInt32Default
	dictionaryBoundsInt64  = dictionaryBoundsInt64Default
	dictionaryBoundsUint32 = dictionaryBoundsUint32Default
	dictionaryBoundsUint64 = dictionaryBoundsUint64Default
	dictionaryLookup32bits = dictionaryLookup32bitsDefault
	dictionaryLookup64bits = dictionaryLookup64bitsDefault
)

func init() {
	switch {
	case cpu.X86.HasAVX512F && cpu.X86.HasAVX512VL:
		dictionaryBoundsInt32 = dictionaryBoundsInt32AVX512
		dictionaryBoundsInt64 = dictionaryBoundsInt64AVX512
		dictionaryBoundsUint32 = dictionaryBoundsUint32AVX512
		dictionaryBoundsUint64 = dictionaryBoundsUint64AVX512
		dictionaryLookup32bits = dictionaryLookup32bitsAVX512
		dictionaryLookup64bits = dictionaryLookup64bitsAVX512
	}
}

func dictionaryBoundsInt32Default(dict []int32, indexes []int32) (min, max int32, err errno) {
	min = dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		v := dict[i]
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max, ok
}

func dictionaryBoundsInt64Default(dict []int64, indexes []int32) (min, max int64, err errno) {
	min = dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		v := dict[i]
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max, ok
}

func dictionaryBoundsUint32Default(dict []uint32, indexes []int32) (min, max uint32, err errno) {
	min = dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		v := dict[i]
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max, ok
}

func dictionaryBoundsUint64Default(dict []uint64, indexes []int32) (min, max uint64, err errno) {
	min = dict[indexes[0]]
	max = min

	for _, i := range indexes[1:] {
		v := dict[i]
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max, ok
}

//go:noescape
func dictionaryBoundsInt32AVX512(dict []int32, indexes []int32) (min, max int32, err errno)

//go:noescape
func dictionaryBoundsInt64AVX512(dict []int64, indexes []int32) (min, max int64, err errno)

//go:noescape
func dictionaryBoundsUint32AVX512(dict []uint32, indexes []int32) (min, max uint32, err errno)

//go:noescape
func dictionaryBoundsUint64AVX512(dict []uint64, indexes []int32) (min, max uint64, err errno)

//go:noescape
func dictionaryLookup32bitsDefault(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionaryLookup32bitsAVX512(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionaryLookup64bitsDefault(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionaryLookup64bitsAVX512(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno

func (d *booleanDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*bool)(rows.index(i, size, offset)) = d.index(j)
	}
}

func (d *int32Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dict := *(*[]uint32)(unsafe.Pointer(&d.values))
	dictionaryLookup32bits(dict, indexes, rows, size, offset).check()
}

func (d *int64Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dict := *(*[]uint64)(unsafe.Pointer(&d.values))
	dictionaryLookup64bits(dict, indexes, rows, size, offset).check()
}

func (d *floatDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dict := *(*[]uint32)(unsafe.Pointer(&d.values))
	dictionaryLookup32bits(dict, indexes, rows, size, offset).check()
}

func (d *doubleDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dict := *(*[]uint64)(unsafe.Pointer(&d.values))
	dictionaryLookup64bits(dict, indexes, rows, size, offset).check()
}

func (d *byteArrayDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		v := d.index(j)
		*(*string)(rows.index(i, size, offset)) = *(*string)(unsafe.Pointer(&v))
	}
}

func (d *uint32Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dictionaryLookup32bits(d.values, indexes, rows, size, offset).check()
}

func (d *uint64Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dictionaryLookup64bits(d.values, indexes, rows, size, offset).check()
}

func (d *int32Dictionary) bounds(indexes []int32) (min, max int32) {
	min, max, err := dictionaryBoundsInt32(d.values, indexes)
	err.check()
	return min, max
}

func (d *int64Dictionary) bounds(indexes []int32) (min, max int64) {
	min, max, err := dictionaryBoundsInt64(d.values, indexes)
	err.check()
	return min, max
}

func (d *uint32Dictionary) bounds(indexes []int32) (min, max uint32) {
	min, max, err := dictionaryBoundsUint32(d.values, indexes)
	err.check()
	return min, max
}

func (d *uint64Dictionary) bounds(indexes []int32) (min, max uint64) {
	min, max, err := dictionaryBoundsUint64(d.values, indexes)
	err.check()
	return min, max
}
