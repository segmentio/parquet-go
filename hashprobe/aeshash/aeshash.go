// Package aeshash implements hashing functions derived from the Go runtime's
// internal hashing based on the support of AES encryption in CPU instructions.
//
// On architecture where the CPU does not provide instructions for AES
// encryption, the aeshash.Enabled function always returns false, and attempting
// to call any other function will trigger a panic.
package aeshash

import "github.com/segmentio/parquet-go/hashprobe/sparse"

func MultiHash32(hashes []uintptr, values []uint32, seed uintptr) {
	MultiHashArray32(hashes, sparse.MakeArray32(values), seed)
}

func MultiHash64(hashes []uintptr, values []uint64, seed uintptr) {
	MultiHashArray64(hashes, sparse.MakeArray64(values), seed)
}

func MultiHash128(hashes []uintptr, values [][16]byte, seed uintptr) {
	MultiHashArray128(hashes, sparse.MakeArray128(values), seed)
}
