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

    CMPQ CX, $8
    JB loop1x4

    MOVQ CX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    VPBROADCASTD size+40(FP), Y0
    VPMULLD scale8x4<>(SB), Y0, Y0

    MOVQ $0xFFFFFFFF, R8
    MOVQ R8, X1
    VPBROADCASTD X1, Y1
    VMOVDQU Y1, Y2

    MOVQ DX, R9
    SHLQ $3, R9
loop8x4:
    VPGATHERDD Y1, (BX)(Y0*1), Y3
    VMOVDQU Y3, (AX)(SI*4)
    VMOVDQU Y2, Y1

    ADDQ R9, BX
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loop8x4
    VZEROUPPER

    CMPQ SI, CX
    JE done

    /*
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
    */

loop1x4:
    MOVL (BX), R8
    MOVL R8, (AX)(SI*4)

    ADDQ DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop1x4
done:
    RET

GLOBL scale8x4<>(SB), RODATA|NOPTR, $32
DATA scale8x4<>+0(SB)/4,  $0
DATA scale8x4<>+4(SB)/4,  $1
DATA scale8x4<>+8(SB)/4,  $2
DATA scale8x4<>+12(SB)/4, $3
DATA scale8x4<>+16(SB)/4, $4
DATA scale8x4<>+20(SB)/4, $5
DATA scale8x4<>+24(SB)/4, $6
DATA scale8x4<>+28(SB)/4, $7
