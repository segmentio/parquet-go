//go:build amd64 && !purego

#include "textflag.h"

// func gatherBitsAVX2(dst []byte, src Uint8Array) int
TEXT ·gatherBitsAVX2(SB), NOSPLIT, $0-56
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), DI
    MOVQ src_array_ptr+24(FP), BX
    MOVQ src_array_len+32(FP), CX
    MOVQ src_array_off+40(FP), DX

    SHLQ $3, DI
    CMPQ DI, CX
    CMOVQLT DI, CX
    MOVQ CX, ret+48(FP)

    CMPQ CX, $0
    JE done

    SHRQ $3, CX
    XORQ SI, SI
    CMPQ CX, $0
    JL tail

    // Make sure `offset` is at least 4 bytes, otherwise VPGATHERDD may read
    // data beyond the end of the program memory and trigger a fault.
    //
    // If the boolean values do not have enough padding we must fallback to the
    // scalar algorithm to be able to load single bytes from memory.
    CMPQ DX, $4
    JB loop8

    VPBROADCASTD src_array_off+40(FP), Y0
    VPMULLD range0n7x32<>(SB), Y0, Y0
    VPCMPEQD Y1, Y1, Y1
    VPCMPEQD Y2, Y2, Y2
avx2loop:
    VPGATHERDD Y1, (BX)(Y0*1), Y3
    VMOVDQU Y2, Y1
    VPSLLD $31, Y3, Y3
    VMOVMSKPS Y3, DI

    MOVB DI, (AX)(SI*1)

    LEAQ (BX)(DX*8), BX
    INCQ SI
    CMPQ SI, CX
    JNE avx2loop
    VZEROUPPER
    JMP tail
loop8:
    LEAQ (BX)(DX*2), DI
    MOVBQZX (BX), R8
    MOVBQZX (BX)(DX*1), R9
    MOVBQZX (DI), R10
    MOVBQZX (DI)(DX*1), R11
    LEAQ (BX)(DX*4), BX
    LEAQ (DI)(DX*4), DI
    MOVBQZX (BX), R12
    MOVBQZX (BX)(DX*1), R13
    MOVBQZX (DI), R14
    MOVBQZX (DI)(DX*1), R15
    LEAQ (BX)(DX*4), BX

    ANDQ $1, R8
    ANDQ $1, R9
    ANDQ $1, R10
    ANDQ $1, R11
    ANDQ $1, R12
    ANDQ $1, R13
    ANDQ $1, R14
    ANDQ $1, R15

    SHLQ $1, R9
    SHLQ $2, R10
    SHLQ $3, R11
    SHLQ $4, R12
    SHLQ $5, R13
    SHLQ $6, R14
    SHLQ $7, R15

    ORQ R9, R8
    ORQ R11, R10
    ORQ R13, R12
    ORQ R15, R14
    ORQ R10, R8
    ORQ R12, R8
    ORQ R14, R8

    MOVB R8, (AX)(SI*1)

    INCQ SI
    CMPQ SI, CX
    JNE loop8
tail:
    SHLQ $3, SI
    MOVQ ret+48(FP), DI
loop1:
    CMPQ SI, DI
    JE done
    MOVB SI, R8
    MOVB SI, CX
    SHRB $3, R8     // x = i / 8
    ANDB $0b111, CX // y = i % 8
    MOVB (BX), R10  // b = src.Index(i)
    ANDB $1, R10
    SHLB CX, R10
    MOVB (AX)(R8*1), R11
    MOVB $1, R12
    SHLB CX, R12
    ANDB R12, R11
    ORB R10, R11
    MOVB R11, (AX)(R8*1)
    ADDQ DX, BX
    INCQ SI
    JMP loop1
done:
    RET

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
