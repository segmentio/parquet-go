//go:build !purego

#include "textflag.h"

// func encodeByteArrayLengthsDefault(length, offset []int32) {
TEXT ·encodeByteArrayLengthsDefault(SB), NOSPLIT, $0-48
    MOVQ length_base+0(FP), AX
    MOVQ length_len+8(FP), CX
    MOVQ offset_base+24(FP), BX
    XORQ SI, SI
    JMP test
loop:
    MOVL 4(BX)(SI*4), DX
    MOVL (BX)(SI*4), DI
    SUBL DI, DX
    MOVL DX, (AX)(SI*4)
    INCQ SI
test:
    CMPQ SI, CX
    JNE loop
    RET

// func encodeByteArrayLengthsAVX2(length, offset []int32) {
TEXT ·encodeByteArrayLengthsAVX2(SB), NOSPLIT, $0-48
    MOVQ length_base+0(FP), AX
    MOVQ length_len+8(FP), CX
    MOVQ offset_base+24(FP), BX
    XORQ SI, SI

    CMPQ CX, $8
    JB test

    MOVQ CX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    VMOVDQU ·rotateLeft32(SB), Y3
    VPXOR X0, X0, X0            // [0,0,0,0,0,0,0,0]
loopAVX2:
    VMOVDQU 4(BX)(SI*4), Y1     // [0,1,2,3,4,5,6,7]
    VPERMD Y1, Y3, Y2           // [7,0,1,2,3,4,5,6]
    VPBLENDD $1, Y0, Y2, Y2     // [x,0,1,2,3,4,5,6]
    VPSUBD Y2, Y1, Y2           // [0,1,2,...] - [x,0,1,...]
    VMOVDQU Y2, (AX)(SI*4)
    VPERMD Y1, Y3, Y0
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX2
    VZEROUPPER
    JMP test
loop:
    MOVL 4(BX)(SI*4), DX
    MOVL (BX)(SI*4), DI
    SUBL DI, DX
    MOVL DX, (AX)(SI*4)
    INCQ SI
test:
    CMPQ SI, CX
    JNE loop
    RET

// func decodeByteArrayLengths(length []int32) {
TEXT ·decodeByteArrayLengths(SB), NOSPLIT, $0-24
    MOVQ length_base+0(FP), AX
    MOVQ length_len+8(FP), BX
    XORQ CX, CX
    XORQ SI, SI

    CMPQ BX, $2
    JB test

    MOVQ BX, DI
    SHRQ $1, DI
    SHLQ $1, DI
loop:
    MOVQ (AX)(SI*4), DX

    MOVL CX, (AX)(SI*4)
    ADDL DX, CX

    SHRQ $32, DX

    MOVL CX, 4(AX)(SI*4)
    ADDL DX, CX

    ADDQ $2, SI
    CMPQ SI, DI
    JNE loop
test:
    CMPQ SI, BX
    JE done
    MOVL CX, (AX)(SI*4)
done:
    RET
