//go:build !purego

package hashprobe

import (
	"golang.org/x/sys/cpu"
)

//go:noescape
func multiProbe64Uint64Default(table []uint64, len, cap int, hashes, keys []uint64, values []int32) int

//go:noescape
func multiLookup64Uint64AVX2(table []uint64, cap int, hashes, keys []uint64, values []int32) int

func multiProbe64Uint64(table []uint64, tableLen, tableCap int, hashes, keys []uint64, values []int32) int {
	// fmt.Println("PROBE:", values)
	// ret := values
	// defer func() { fmt.Println("RETURN:", ret) }()

	if len(values) >= 8 && cpu.X86.HasAVX2 {
		i := (len(values) / 8) * 8
		n := multiLookup64Uint64AVX2(table, tableCap, hashes[:i:i], keys[:i:i], values[:i:i])
		//fmt.Println("match:", n)

		if n != i {
			for j := 0; j < i; {
				for j < i && values[j] >= 0 {
					j++
				}

				if j == i {
					break
				}

				k := j + 1
				for k < i && values[k] < 0 {
					k++
				}

				//fmt.Printf("insert: %d:%d %d\n", j, k, values[j:k:k])
				tableLen = multiProbe64Uint64Default(table, tableLen, tableCap, hashes[j:k:k], keys[j:k:k], values[j:k:k])
				//fmt.Printf(">>> %d\n", values[j:k:k])
				j = k
			}
		}

		hashes, keys, values = hashes[i:], keys[i:], values[i:]
	}

	//fmt.Println("tail:", values)
	return multiProbe64Uint64Default(table, tableLen, tableCap, hashes, keys, values)
}
