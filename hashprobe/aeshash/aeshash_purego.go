//go:build purego || !amd64

package aeshash

// Enabled always returns false since we assume that AES instructions are not
// available by default.
func Enabled() bool { return false }

const unsupported = "BUG: AES hash is not available on this platform"

func Sum32Uint32(value, seed uint32) uint32 { panic(unsupported) }

func MultiSum32Uint32(hashes, values []uint32, seed uint32) { panic(unsupported) }

func Sum64Uint64(value, seed uint64) uint64 { panic(unsupported) }

func MultiSum64Uint64(hashes, values []uint64, seed uint64) { panic(unsupported) }
