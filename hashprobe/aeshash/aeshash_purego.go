//go:build purego || !amd64

package aeshash

import "github.com/segmentio/parquet-go/hashprobe/sparse"

// Enabled always returns false since we assume that AES instructions are not
// available by default.
func Enabled() bool { return false }

const unsupported = "BUG: AES hash is not available on this platform"

func Hash32(value uint32, seed uintptr) uintptr { panic(unsupported) }

func Hash64(value uint64, seed uintptr) uintptr { panic(unsupported) }

func Hash128(value [16]byte, seed uintptr) uintptr { panic(unsupported) }

func MultiHashArray32(hashes []uintptr, values sparse.Array32, seed uintptr) { panic(unsupported) }

func MultiHashArray64(hashes []uintptr, values sparse.Array64, seed uintptr) { panic(unsupported) }

func MultiHashArray128(hashes []uintptr, values sparse.Array128, seed uintptr) { panic(unsupported) }
