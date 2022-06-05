//go:build !purego

#include "textflag.h"

// parquet.Value is a 24 bytes type on AMD64
#define sizeOfValue 24

// This function is an optimized implementation of the clearValuesKind function
// which erases the content of a values slice and sets all values to the given
// kind.
//
// The optimizations relies on the fact that we can pack 4 parquet.Value values
// into a 3 YMM registers (24 x 4 = 32 x 3 = 96). The content of the vector
// registers is initialized to zero, except for the 4 locations where the value
// kind must be written: offsets 16, 40, 64, and 88.
//
// func clearValuesKindAVX2(values []Value, kind Kind)
TEXT ·clearValuesKindAVX2(SB), NOSPLIT, $0-32
    MOVQ values+0(FP), AX
    MOVQ values+8(FP), BX
    MOVBQZX kind+24(FP), CX

    MOVQ $0xFFFFFFFFFFFFFFFF, DI
    XORQ DI, CX // flip the bits of `kind`
    XORQ DX, DX // zero
    XORQ SI, SI // byte index
    MOVQ BX, DI // byte count
    IMULQ $sizeOfValue, DI

    CMPQ BX, $4
    JB test1x24

    MOVQ BX, R8
    SHRQ $2, R8
    SHLQ $2, R8
    IMULQ $sizeOfValue, R8

    MOVQ CX, X0
    VPBROADCASTD X0, Y0
    VMOVDQU Y0, Y1
    VMOVDQU Y0, Y2
    VPXOR Y3, Y3, Y3

    VMOVDQU valueKindMasks<>+0(SB), Y4
    VMOVDQU valueKindMasks<>+32(SB), Y5
    VMOVDQU valueKindMasks<>+64(SB), Y6

    VPBLENDVB Y4, Y0, Y3, Y0
    VPBLENDVB Y5, Y1, Y3, Y1
    VPBLENDVB Y6, Y2, Y3, Y2
loop4x24:
    VMOVDQU Y0, 0(AX)(SI*1)
    VMOVDQU Y1, 32(AX)(SI*1)
    VMOVDQU Y2, 64(AX)(SI*1)
    ADDQ $4*sizeOfValue, SI
    CMPQ SI, R8
    JNE loop4x24
    VZEROUPPER
    JMP test1x24

loop1x24:
    MOVQ DX, 0(AX)(SI*1)
    MOVQ DX, 8(AX)(SI*1)
    MOVQ CX, 16(AX)(SI*1)
    ADDQ $sizeOfValue, SI
test1x24:
    CMPQ SI, DI
    JNE loop1x24
    RET

GLOBL valueKindMasks<>(SB), RODATA|NOPTR, $96
DATA valueKindMasks<>+16(SB)/1, $0xFF
DATA valueKindMasks<>+40(SB)/1, $0xFF
DATA valueKindMasks<>+64(SB)/1, $0xFF
DATA valueKindMasks<>+88(SB)/1, $0xFF
