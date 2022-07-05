//go:build purego || !amd64

package aeshash

// Enabled always returns false since we assume that AES instructions are not
// available by default.
func Enabled() bool { return false }

const unsupported = "BUG: AES hash is not available on this platform"

func Hash32(value uint32, seed uintptr) uintptr { panic(unsupported) }

func MultiHash32(hashes []uintptr, values []uint32, seed uintptr) { panic(unsupported) }

func Hash64(value uint64, seed uintptr) uintptr { panic(unsupported) }

func MultiHash64(hashes []uintptr, values []uint64, seed uintptr) { panic(unsupported) }
