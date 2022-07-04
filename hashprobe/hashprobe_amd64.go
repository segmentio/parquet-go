//go:build !purego

package hashprobe

//go:noescape
func multiProbe64Uint64Default(table []uint64, len, cap int, hashes, keys []uint64, values []int32) int

//go:noescape
func multiProbe64Uint64AVX2(table []uint64, len, cap int, hashes, keys []uint64, values []int32) int

func multiProbe64Uint64(table []uint64, tableLen, tableCap int, hashes, keys []uint64, values []int32) int {
	// if len(hashes) >= 8 && cpu.X86.HasAVX2 {
	// 	i := (len(hashes) / 8) * 8
	// 	tableLen += multiProbe64Uint64AVX2(table, tableLen, tableCap, hashes[:i], keys[:i], values[:i])
	// 	hashes, keys, values = hashes[i:], keys[i:], values[i:]
	// }
	return multiProbe64Uint64Default(table, tableLen, tableCap, hashes, keys, values)
}
