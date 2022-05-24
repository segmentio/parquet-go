//go:build !purego

#include "textflag.h"

// func writeValuesInt32(values []int32, rows array, size, offset uintptr)
TEXT ·writeValuesInt32(SB), NOSPLIT, $0-56
    MOVQ values_base+0(FP), AX
    MOVQ rows_base+24(FP), BX
    MOVQ rows_len+32(FP), CX
    MOVQ size+40(FP), DX
    MOVQ offset+48(FP), DI

    XORQ SI, SI
    ADDQ DI, BX
loop:
    MOVL (BX), R8
    MOVL R8, (AX)

    ADDQ $4, AX
    ADDQ DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop
    RET
