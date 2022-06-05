//go:build !purego

package parquet

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

//go:noescape
func int32DictionaryLookupAVX2(dict []int32, indexes []int32, values array, size, offset uintptr) errno
