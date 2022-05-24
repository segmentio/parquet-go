//go:build !amd64 || purego

package parquet

func writeValuesInt32(values []int32, rows array, size, offset uintptr) {
	for i := range values {
		values[i] = *(*int32)(rows.index(i, size, offset))
	}
}
