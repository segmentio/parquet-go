//go:build !purego

#include "textflag.h"

// func writeValuesInt32(values []int32, rows array, size, offset uintptr)
TEXT ·writeValuesInt32(SB), NOSPLIT, $0-56
    MOVQ values_base+0(FP), AX
    MOVQ rows_base+24(FP), BX
    MOVQ rows_len+32(FP), CX
    MOVQ size+40(FP), DX

    XORQ SI, SI
    ADDQ offset+48(FP), BX

    CMPQ CX, $0
    JE done

    CMPQ CX, $4
    JB loop1x4

    MOVQ CX, DI
    SHRQ $2, DI
    SHLQ $2, DI
loop4x4:
    MOVL (BX), R8
    ADDQ DX, BX

    MOVL (BX), R9
    ADDQ DX, BX

    MOVL (BX), R10
    ADDQ DX, BX

    MOVL (BX), R11
    ADDQ DX, BX

    MOVL R8, (AX)(SI*4)
    MOVL R9, 4(AX)(SI*4)
    MOVL R10, 8(AX)(SI*4)
    MOVL R11, 12(AX)(SI*4)

    ADDQ $4, SI
    CMPQ SI, DI
    JNE loop4x4

    CMPQ SI, CX
    JE done
loop1x4:
    MOVL (BX), R8
    MOVL R8, (AX)(SI*4)

    ADDQ DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop1x4
done:
    RET
