//go:build !purego

#include "textflag.h"

// func memset(dst []byte, src byte)
TEXT ·memset(SB), NOSPLIT, $0-32
    MOVQ dst+0(FP), AX
    MOVQ dst+8(FP), BX
    MOVBQZX src+24(FP), CX
    XORQ SI, SI

    CMPQ BX, $8
    JBE test

    CMPQ BX, $64
    JB init8

    CMPB ·hasAVX2(SB), $0
    JE init8

    MOVQ BX, DX
    SHRQ $6, DX
    SHLQ $6, DX
    MOVQ CX, X0
    VPBROADCASTB X0, Y0
loop64:
    VMOVDQU Y0, (AX)(SI*1)
    VMOVDQU Y0, 32(AX)(SI*1)
    ADDQ $64, SI
    CMPQ SI, DX
    JNE loop64
    VMOVDQU Y0, -64(AX)(BX*1)
    VMOVDQU Y0, -32(AX)(BX*1)
    VZEROUPPER
    RET
init8:
    MOVQ $0x0101010101010101, R8
    IMULQ R8, CX

    MOVQ BX, DX
    SHRQ $3, DX
    SHLQ $3, DX
loop8:
    MOVQ CX, (AX)(SI*1)
    ADDQ $8, SI
    CMPQ SI, DX
    JNE loop8
    JMP test
loop:
    MOVB CX, (AX)(SI*1)
    INCQ SI
test:
    CMPQ SI, BX
    JNE loop
    RET
