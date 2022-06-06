//go:build !purego

package parquet

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

var (
	dictionary32bitsLookup func(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno
	dictionary64bitsLookup func(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno
)

func init() {
	if cpu.X86.HasAVX512F && cpu.X86.HasAVX512VL {
		dictionary32bitsLookup = dictionary32bitsLookupAVX512
		dictionary64bitsLookup = dictionary64bitsLookupAVX512
	} else {
		dictionary32bitsLookup = dictionary32bitsLookupDefault
		dictionary64bitsLookup = dictionary64bitsLookupDefault
	}
}

//go:noescape
func dictionary32bitsLookupDefault(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionary64bitsLookupDefault(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionary32bitsLookupAVX512(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionary64bitsLookupAVX512(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno

func (d *booleanDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*bool)(rows.index(i, size, offset)) = d.index(j)
	}
}

func (d *int32Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dict := *(*[]uint32)(unsafe.Pointer(&d.values))
	dictionary32bitsLookup(dict, indexes, rows, size, offset).check()
}

func (d *int64Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dict := *(*[]uint64)(unsafe.Pointer(&d.values))
	dictionary64bitsLookup(dict, indexes, rows, size, offset).check()
}

func (d *floatDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dict := *(*[]uint32)(unsafe.Pointer(&d.values))
	dictionary32bitsLookup(dict, indexes, rows, size, offset).check()
}

func (d *doubleDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dict := *(*[]uint64)(unsafe.Pointer(&d.values))
	dictionary64bitsLookup(dict, indexes, rows, size, offset).check()
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
	dictionary32bitsLookup(d.values, indexes, rows, size, offset).check()
}

func (d *uint64Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	dictionary64bitsLookup(d.values, indexes, rows, size, offset).check()
}
