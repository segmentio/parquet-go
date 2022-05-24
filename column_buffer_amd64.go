//go:build !purego

package parquet

//go:noescape
func writeValuesInt32(values []int32, rows array, size, offset uintptr)
