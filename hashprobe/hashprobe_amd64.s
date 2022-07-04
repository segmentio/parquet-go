//go:build !purego

#include "textflag.h"

// func multiProbe64Uint64Default(table []uint64, len, cap int, hashes, keys []uint64, values []int32) int
TEXT ·multiProbe64Uint64Default(SB), NOSPLIT, $0-120
    MOVQ table_base+0(FP), AX
    MOVQ len+24(FP), BX
    MOVQ cap+32(FP), CX
    MOVQ hashes_base+40(FP), DX
    MOVQ hashes_len+48(FP), DI
    MOVQ keys_base+64(FP), R8
    MOVQ values_base+88(FP), R9

    MOVQ CX, R10
    SHRQ $6, R10 // offset = cap / 64

    MOVQ CX, R11
    DECQ R11 // modulo = cap - 1

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R12 // hash
probe:
    MOVQ R12, R13
    ANDQ R11, R13 // position = hash & modulo

    MOVQ R13, R14
    MOVQ R13, R15
    SHRQ $6, R14        // index = position / 64
    ANDQ $0b111111, R15 // shift = position % 64

    SHLQ $1, R13  // position *= 2
    ADDQ R10, R13 // position += offset

    MOVQ (AX)(R14*8), CX
    BTSQ R15, CX
    JNC insert // table[index] & 1<<shift == 0 ?

    MOVQ (AX)(R13*8), CX
    CMPQ (R8)(SI*8), CX
    JNE nextprobe // table[position] != keys[i] ?
    MOVL 8(AX)(R13*8), R13
    MOVL R13, (R9)(SI*4)
next:
    INCQ SI
test:
    CMPQ SI, DI
    JNE loop
    MOVQ BX, ret+112(FP)
    RET
insert:
    MOVQ CX, (AX)(R14*8)
    MOVQ (R8)(SI*8), R14 // key
    MOVQ R14, (AX)(R13*8)
    MOVQ BX, 8(AX)(R13*8)
    MOVL BX, (R9)(SI*4)
    INCQ BX // len++
    JMP next
nextprobe:
    INCQ R12
    JMP probe

GLOBL permq2dh<>(SB), RODATA|NOPTR, $32
DATA permq2dh<>+0(SB)/4, $1
DATA permq2dh<>+4(SB)/4, $3
DATA permq2dh<>+8(SB)/4, $5
DATA permq2dh<>+12(SB)/4, $7
DATA permq2dh<>+16(SB)/4, $8
DATA permq2dh<>+20(SB)/4, $8
DATA permq2dh<>+24(SB)/4, $8
DATA permq2dh<>+28(SB)/4, $8

// func multiLookup64Uint64AVX2(table []uint64, cap int, hashes, keys []uint64, values []int32) int
TEXT ·multiLookup64Uint64AVX2(SB), NOSPLIT, $0-112
    MOVQ table_base+0(FP), AX
    XORQ BX, BX
    MOVQ cap+24(FP), CX
    MOVQ hashes_base+32(FP), DX
    MOVQ hashes_len+40(FP), DI
    MOVQ keys_base+56(FP), R8
    MOVQ values_base+80(FP), R9

    MOVQ CX, R10
    SHRQ $6, R10 // offset = cap / 64
    MOVQ R10, X0
    VPBROADCASTQ X0, Y0

    MOVQ CX, R11
    DECQ R11 // modulo = cap - 1
    MOVQ R11, X1
    VPBROADCASTQ X1, Y1

    VPCMPEQQ Y2, Y2, Y2 // all ones
    VPSRLQ $58, Y2, Y3  // [0b00111111]
    VPSRLQ $63, Y2, Y4  // [0b00000001]

    VMOVDQA permq2dh<>(SB), Y7

    XORQ SI, SI // i = 0
loop:
    VPCMPEQQ X15, X15, X15
    VMOVDQA Y2, Y5
    VMOVDQA Y2, Y6
    VMOVDQU (DX)(SI*8), Y8 // hash
    VMOVDQU (R8)(SI*8), Y9 // key

    VPAND Y1, Y8, Y10   // position = [hash] & [modulo]
    VPSRLQ $6, Y10, Y11 // index = position / 64
    VPAND Y3, Y10, Y12  // shift = position % 64
    VPSLLQ $1, Y10, Y10 // position *= 2
    VPADDQ Y0, Y10, Y10 // position += offset

    VPGATHERQQ Y5, (AX)(Y11*8), Y13 // table[index]
    VPSRLVQ Y12, Y13, Y13
    VPSLLQ $63, Y13, Y13
    VMOVMSKPD Y13, CX
    POPCNTQ CX, CX
    ADDQ CX, BX

    VPGATHERQQ Y6, (AX)(Y10*8), Y14 // table[position]
    VPCMPEQQ Y9, Y14, Y14
    VPAND Y14, Y13, Y14
    VPERMD Y14, Y7, Y14
    VPGATHERQD X14, 8(AX)(Y10*8), X15
    VMOVDQU X15, (R9)(SI*4)

    ADDQ $4, SI
    CMPQ SI, DI
    JNE loop
    MOVQ BX, ret+104(FP)
    RET
