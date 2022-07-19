//go:build purego || !amd64

package hashprobe

import (
	"github.com/segmentio/parquet-go/sparse"
)

func multiProbe32(table []table32Group, numKeys int, hashes []uintptr, keys sparse.Uint32Array, values []int32) int {
	return multiProbe32Default(table, numKeys, hashes, keys, values)
}

func multiProbe64(table []table64Group, numKeys int, hashes []uintptr, keys sparse.Uint64Array, values []int32) int {
	return multiProbe64Default(table, numKeys, hashes, keys, values)
}

func multiProbe128(table []byte, tableCap, tableLen int, hashes []uintptr, keys sparse.Uint128Array, values []int32) int {
	return multiProbe128Default(table, tableCap, tableLen, hashes, keys, values)
}

func probeStringTable16(table []stringGroup16, hash uintptr, key string, newValue int32) (value int32, insert int) {
	return probeStringTable16Default(table, hash, key, newValue)
}

func probeStringTable32(table []stringGroup32, hash uintptr, key string, newValue int32) (value int32, insert int) {
	return probeStringTable32Default(table, hash, key, newValue)
}
