//go:build !purego

package parquet

//go:noescape
func memset(dst []byte, src byte)
