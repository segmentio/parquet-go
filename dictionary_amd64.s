//go:build !purego

#include "textflag.h"

// func int32DictionaryLookupAVX2(dict, indexes []int32, values array, size, offset uintptr) errno
TEXT ·int32DictionaryLookupAVX2(SB), NOSPLIT, $0-88
    MOVQ dict+0(FP), AX
    MOVQ dict+8(FP), BX

    MOVQ indexes+24(FP), CX
    MOVQ indexes+32(FP), DX

    MOVQ values+48(FP), R8
    MOVQ size+64(FP), R9
    ADDQ offset+72(FP), R8

    XORQ SI, SI
    MOVQ SI, ret+80(FP)
    JMP test
loop:
    MOVL (CX)(SI*4), R10
    MOVL (AX)(R11*4), R10
    MOVL R10, (R8)
    ADDQ R9, R8
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    RET
indexOutOfBounds:
    MOVQ $1, AX
    MOVQ AX, ret+80(FP)
    RET
