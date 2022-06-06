//go:build !purego

package parquet

import (
	"unsafe"
)

//go:noescape
func dictionaryBoundsInt32(dict []int32, indexes []int32) (min, max int32, err errno)

//go:noescape
func dictionaryBoundsInt64(dict []int64, indexes []int32) (min, max int64, err errno)

//go:noescape
func dictionaryBoundsFloat32(dict []float32, indexes []int32) (min, max float32, err errno)

//go:noescape
func dictionaryBoundsFloat64(dict []float64, indexes []int32) (min, max float64, err errno)

//go:noescape
func dictionaryBoundsUint32(dict []uint32, indexes []int32) (min, max uint32, err errno)

//go:noescape
func dictionaryBoundsUint64(dict []uint64, indexes []int32) (min, max uint64, err errno)

//go:noescape
func dictionaryLookup32bits(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno

//go:noescape
func dictionaryLookup64bits(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno

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

func (d *floatDictionary) bounds(indexes []int32) (min, max float32) {
	min, max, err := dictionaryBoundsFloat32(d.values, indexes)
	err.check()
	return min, max
}

func (d *doubleDictionary) bounds(indexes []int32) (min, max float64) {
	min, max, err := dictionaryBoundsFloat64(d.values, indexes)
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
