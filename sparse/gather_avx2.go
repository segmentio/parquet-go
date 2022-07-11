//go:build amd64 && !purego

package sparse

//go:noescape
func gatherBitsAVX2(dst []byte, src Uint8Array) int

//go:noescape
func gather32AVX2(dst []uint32, src Uint32Array) int

//go:noescape
func gather64AVX2(dst []uint64, src Uint64Array) int
