//go:build !purego

#include "textflag.h"

// func writeValuesBitpack(values unsafe.Pointer, rows array, size, offset uintptr)
TEXT ·writeValuesBitpack(SB), NOSPLIT, $0-40
    MOVQ values_base+0(FP), AX
    MOVQ rows_base+8(FP), BX
    MOVQ rows_len+16(FP), CX
    MOVQ size+24(FP), DX
    MOVQ offset+32(FP), DI

    CMPQ CX, $0
    JNE init
    RET
init:
    ADDQ DI, BX
    SHRQ $3, CX
    XORQ SI, SI

    // Make sure `size - offset` is at least 4 bytes, otherwise VPGATHERDD
    // may read data beyond the end of the program memory and trigger a fault.
    //
    // If the boolean values do not have enough padding we must fallback to the
    // scalar algorithm to be able to load single bytes from memory.
    MOVQ DX, R8
    SUBQ DI, R8
    CMPQ R8, $4
    JB loop

    VPBROADCASTD size+24(FP), Y0
    VPMULLD scale8x4<>(SB), Y0, Y0
    VPCMPEQD Y1, Y1, Y1
    VPCMPEQD Y2, Y2, Y2
    VPCMPEQD Y3, Y3, Y3
    VPSRLD $31, Y3, Y3
avx2loop:
    VPGATHERDD Y1, (BX)(Y0*1), Y4
    VMOVDQU Y2, Y1
    VPAND Y3, Y4, Y4
    VPSLLD $31, Y4, Y4
    VMOVMSKPS Y4, DI

    MOVB DI, (AX)(SI*1)

    LEAQ (BX)(DX*8), BX
    INCQ SI
    CMPQ SI, CX
    JNE avx2loop
    VZEROUPPER
    RET
loop:
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
    JNE loop
    RET

// func writeValues32bits(values unsafe.Pointer, rows array, size, offset uintptr)
TEXT ·writeValues32bits(SB), NOSPLIT, $0-40
    MOVQ values_base+0(FP), AX
    MOVQ rows_base+8(FP), BX
    MOVQ rows_len+16(FP), CX
    MOVQ size+24(FP), DX

    XORQ SI, SI
    ADDQ offset+32(FP), BX

    CMPQ CX, $0
    JE done

    CMPQ CX, $8
    JB loop1x4

    MOVQ CX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    VPBROADCASTD size+24(FP), Y0
    VPMULLD scale8x4<>(SB), Y0, Y0
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

// func writeValues64bits(values unsafe.Pointer, rows array, size, offset uintptr)
TEXT ·writeValues64bits(SB), NOSPLIT, $0-40
    MOVQ values_base+0(FP), AX
    MOVQ rows_base+8(FP), BX
    MOVQ rows_len+16(FP), CX
    MOVQ size+24(FP), DX

    XORQ SI, SI
    ADDQ offset+32(FP), BX

    CMPQ CX, $0
    JE done

    CMPQ CX, $4
    JB loop1x8

    MOVQ CX, DI
    SHRQ $2, DI
    SHLQ $2, DI

    VPBROADCASTQ size+24(FP), Y0
    VPMULLQ scale4x8<>(SB), Y0, Y0
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
    RET

// func writeValues128bits(values unsafe.Pointer, rows array, size, offset uintptr)
TEXT ·writeValues128bits(SB), NOSPLIT, $0-40
    MOVQ values_base+0(FP), AX
    MOVQ rows_base+8(FP), BX
    MOVQ rows_len+16(FP), CX
    MOVQ size+24(FP), DX

    XORQ SI, SI
    ADDQ offset+32(FP), BX

    CMPQ CX, $0
    JE done

    CMPQ CX, $2
    JB loop1x16

    MOVQ CX, DI
    SHRQ $1, DI
    SHLQ $1, DI
loop2x16:
    VMOVDQU (BX), X0
    VMOVDQU (BX)(DX*1), X1

    VMOVDQU X0, (AX)
    VMOVDQU X1, 16(AX)

    ADDQ $32, AX
    LEAQ (BX)(DX*2), BX
    ADDQ $2, SI
    CMPQ SI, DI
    JNE loop2x16
    VZEROUPPER

    CMPQ SI, CX
    JE done
loop1x16:
    MOVOU (BX), X0
    MOVOU X0, (AX)

    ADDQ $16, AX
    ADDQ DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop1x16
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

GLOBL scale4x8<>(SB), RODATA|NOPTR, $32
DATA scale4x8<>+0(SB)/8,  $0
DATA scale4x8<>+8(SB)/8,  $1
DATA scale4x8<>+16(SB)/8, $2
DATA scale4x8<>+24(SB)/8, $3
