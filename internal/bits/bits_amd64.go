package bits

import "golang.org/x/sys/cpu"

var hasAVX512 = cpu.X86.HasAVX512 &&
	cpu.X86.HasAVX512F &&
	cpu.X86.HasAVX512VL

var hasAVX512MinMaxBool = hasAVX512 &&
	cpu.X86.HasAVX512VPOPCNTDQ

// The use AVX-512 instructions in the minBOol algorithm relies operations
// that are avilable in the AVX512BW extension:
// * VPCMPUB
// * KMOVQ
var hasAVX512MinBool = hasAVX512 &&
	cpu.X86.HasAVX512BW

// The use AVX-512 instructions in the maxBOol algorithm relies operations
// that are avilable in the AVX512BW extension:
// * VPCMPUB
// * KMOVQ
var hasAVX512MaxBool = hasAVX512 &&
	cpu.X86.HasAVX512BW

// The use AVX-512 instructions in the countByte algorithm relies operations
// that are avilable in the AVX512BW extension:
// * VPCMPUB
// * KMOVQ
//
// Note that the function will fallback to an AVX2 version if those instructions
// are not available.
var hasAVX512CountByte = hasAVX512 &&
	cpu.X86.HasAVX512BW
