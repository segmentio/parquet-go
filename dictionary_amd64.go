//go:build !purego

package parquet

import "unsafe"

func (d *booleanDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*bool)(rows.index(i, size, offset)) = d.index(j)
	}
}

func (d *int32Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*int32)(rows.index(i, size, offset)) = d.index(j)
	}
}

func (d *int64Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*int64)(rows.index(i, size, offset)) = d.index(j)
	}
}

func (d *floatDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*float32)(rows.index(i, size, offset)) = d.index(j)
	}
}

func (d *doubleDictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*float64)(rows.index(i, size, offset)) = d.index(j)
	}
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
	for i, j := range indexes {
		*(*uint32)(rows.index(i, size, offset)) = d.index(j)
	}
}

func (d *uint64Dictionary) lookup(indexes []int32, rows array, size, offset uintptr) {
	checkLookupIndexBounds(indexes, rows)
	for i, j := range indexes {
		*(*uint64)(rows.index(i, size, offset)) = d.index(j)
	}
}
