//go:build !purego

package parquet

import "golang.org/x/sys/cpu"

var (
	// This variable is used in x86 assembly source files to gate the use of
	// AVX2 instructions depending on whether the CPU supports it.
	hasAVX2 = cpu.X86.HasAVX2
)
