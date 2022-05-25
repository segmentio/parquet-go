//go:build !purego

package parquet

//go:noescape
func writeValuesBits(values []byte, rows array, size, offset uintptr)

//go:noescape
func writeValuesInt32(values []int32, rows array, size, offset uintptr)
