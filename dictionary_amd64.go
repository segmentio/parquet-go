//go:build !purego

package parquet

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

var (
	dictionaryBoundsInt32  = dictionaryBoundsInt32Default
	dictionaryLookup32bits = dictionaryLookup32bitsDefault
	dictionaryLookup64bits = dictionaryLookup64bitsDefault
)

func init() {
	switch {
	case cpu.X86.HasAVX512F && cpu.X86.HasAVX512VL:
		dictionaryLookup32bits = dictionaryLookup32bitsAVX512
		dictionaryLookup64bits = dictionaryLookup64bitsAVX512
	}
}

//go:noescape
func dictionaryBoundsInt32Default(dict []int32, indexes []int32) (min, max int32, err errno)

//go:noescape
func dictionaryLookup32bitsDefault(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionaryLookup64bitsDefault(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionaryLookup32bitsAVX512(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno

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
