//go:build purego || !amd64

package hashprobe

func multiProbe32(table []table32Group, numKeys int, hashes []uintptr, keys []uint32, values []int32) int {
	return multiProbe32Default(table, numKeys, hashes, keys, values)
}

func multiProbe64(table []table64Group, numKeys int, hashes []uintptr, keys []uint64, values []int32) int {
	return multiProbe64Default(table, numKeys, hashes, keys, values)
}

func multiProbe128(table []table128Group, numKeys int, hashes []uintptr, keys [][16]byte, values []int32) int {
	return multiProbe128Default(table, numKeys, hashes, keys, values)
}
