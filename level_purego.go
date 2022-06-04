//go:build purego || !amd64

package parquet

func memset(dst []byte, src byte) {
	for i := range dst {
		dst[i] = src
	}
}
