//go:build !purego

package aeshash

import "golang.org/x/sys/cpu"

var useAesHash = cpu.X86.HasAES && cpu.X86.HasSSSE3 && cpu.X86.HasSSE41

// Enabled returns true if AES hash is available on the system.
//
// The function uses the same logic than the Go runtime since we depend on
// it the AES hash state being initialized.
//
// See https://go.dev/src/runtime/alg.go
func Enabled() bool { return useAesHash }

//go:noescape
func Sum64Uint64(value, seed uint64) uint64

//go:noescape
func MultiSum64Uint64(hashes, values []uint64, seed uint64)
