//go:build amd64 && !purego

#include "textflag.h"

// func gather32AVX2(dst []uint32, src Uint32Array) int
TEXT ·gather32AVX2(SB), NOSPLIT, $0-56
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), CX
    MOVQ src_array_ptr+24(FP), BX
    MOVQ src_array_len+32(FP), DI
    MOVQ src_array_off+40(FP), DX
    XORQ SI, SI

    CMPQ DI, CX
    CMOVQLT DI, CX

    CMPQ CX, $0
    JE done

    CMPQ CX, $8
    JB loop1x4

    MOVQ CX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    VPBROADCASTD src_array_off+40(FP), Y0
    VPMULLD range0n7x32<>(SB), Y0, Y0
    VPCMPEQD Y1, Y1, Y1
    VPCMPEQD Y2, Y2, Y2
loop8x4:
    VPGATHERDD Y1, (BX)(Y0*1), Y3
    VMOVDQU Y3, (AX)(SI*4)
    VMOVDQU Y2, Y1

    LEAQ (BX)(DX*8), BX
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loop8x4
    VZEROUPPER

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
    MOVQ CX, ret+48(FP)
    RET

// func gather64AVX2(dst []uint64, src Uint64Array) int
TEXT ·gather64AVX2(SB), NOSPLIT, $0-56
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), CX
    MOVQ src_array_ptr+24(FP), BX
    MOVQ src_array_len+32(FP), DI
    MOVQ src_array_off+40(FP), DX
    XORQ SI, SI

    CMPQ DI, CX
    CMOVQLT DI, CX

    CMPQ CX, $0
    JE done

    CMPQ CX, $4
    JB loop1x8

    MOVQ CX, DI
    SHRQ $2, DI
    SHLQ $2, DI

    VPBROADCASTQ src_array_off+40(FP), Y0
    VPMULLD range0n3x64<>(SB), Y0, Y0
    VPCMPEQD Y1, Y1, Y1
    VPCMPEQD Y2, Y2, Y2
loop4x8:
    VPGATHERQQ Y1, (BX)(Y0*1), Y3
    VMOVDQU Y3, (AX)(SI*8)
    VMOVDQU Y2, Y1

    LEAQ (BX)(DX*4), BX
    ADDQ $4, SI
    CMPQ SI, DI
    JNE loop4x8
    VZEROUPPER

    CMPQ SI, CX
    JE done
loop1x8:
    MOVQ (BX), R8
    MOVQ R8, (AX)(SI*8)

    ADDQ DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop1x8
done:
    MOVQ CX, ret+48(FP)
    RET

GLOBL range0n3x64<>(SB), RODATA|NOPTR, $32
DATA range0n3x64<>+0(SB)/8,  $0
DATA range0n3x64<>+8(SB)/8,  $1
DATA range0n3x64<>+16(SB)/8, $2
DATA range0n3x64<>+24(SB)/8, $3

GLOBL range0n7x32<>(SB), RODATA|NOPTR, $32
DATA range0n7x32<>+0(SB)/4, $0
DATA range0n7x32<>+4(SB)/4, $1
DATA range0n7x32<>+8(SB)/4, $2
DATA range0n7x32<>+12(SB)/4, $3
DATA range0n7x32<>+16(SB)/4, $4
DATA range0n7x32<>+20(SB)/4, $5
DATA range0n7x32<>+24(SB)/4, $6
DATA range0n7x32<>+28(SB)/4, $7
