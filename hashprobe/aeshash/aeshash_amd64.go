//go:build !purego

package aeshash

import (
	"github.com/segmentio/parquet-go/hashprobe/sparse"
	"golang.org/x/sys/cpu"
)

// Enabled returns true if AES hash is available on the system.
//
// The function uses the same logic than the Go runtime since we depend on
// the AES hash state being initialized.
//
// See https://go.dev/src/runtime/alg.go
func Enabled() bool { return cpu.X86.HasAES && cpu.X86.HasSSSE3 && cpu.X86.HasSSE41 }

//go:noescape
func Hash32(value uint32, seed uintptr) uintptr

//go:noescape
func Hash64(value uint64, seed uintptr) uintptr

//go:noescape
func Hash128(value [16]byte, seed uintptr) uintptr

//go:noescape
func MultiHashArray32(hashes []uintptr, values sparse.Array32, seed uintptr)

//go:noescape
func MultiHashArray64(hashes []uintptr, values sparse.Array64, seed uintptr)

//go:noescape
func MultiHashArray128(hashes []uintptr, values sparse.Array128, seed uintptr)
