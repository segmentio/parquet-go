//go:build !purego

#include "textflag.h"

#define errnoIndexOutOfBounds 1

// func dictionaryBoundsInt32Default(dict []int32, indexes []int32) (min, max int32, err errno)
TEXT ·dictionaryBoundsInt32Default(SB), NOSPLIT, $0-64
    MOVQ dict+0(FP), AX
    MOVQ dict+8(FP), BX

    MOVQ indexes+24(FP), CX
    MOVQ indexes+32(FP), DX

    XORQ R10, R10 // min
    XORQ R11, R11 // max
    XORQ R12, R12 // err

    XORQ SI, SI
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVL (AX)(DI*4), DI
    CMPL DI, R10
    CMOVLLT DI, R10
    CMPL DI, R11
    CMOVLGT DI, R11
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
return:
    MOVL R10, ret+48(FP)
    MOVL R11, ret+52(FP)
    MOVQ R12, ret+56(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, R12
    JMP return

// The lookup functions provide optimized versions of the dictionary index
// lookup logic.
//
// When AVX512 is available, the AVX512 versions of the functions are used
// which use the VPGATHER* instructions to perform 8 parallel lookups of the
// values in the dictionary, then VPSCATTER* to do 8 parallel writes to the
// sparse output buffer.

// func dictionaryLookup32bitsDefault(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno
TEXT ·dictionaryLookup32bitsDefault(SB), NOSPLIT, $0-88
    MOVQ dict+0(FP), AX
    MOVQ dict+8(FP), BX

    MOVQ indexes+24(FP), CX
    MOVQ indexes+32(FP), DX

    MOVQ values+48(FP), R8
    MOVQ size+64(FP), R9
    ADDQ offset+72(FP), R8

    XORQ SI, SI
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVL (AX)(DI*4), DI
    MOVL DI, (R8)
    ADDQ R9, R8
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    XORQ AX, AX
    MOVQ AX, ret+80(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, AX
    MOVQ AX, ret+80(FP)
    RET

// func dictionaryLookup64bitsDefault(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno
TEXT ·dictionaryLookup64bitsDefault(SB), NOSPLIT, $0-88
    MOVQ dict+0(FP), AX
    MOVQ dict+8(FP), BX

    MOVQ indexes+24(FP), CX
    MOVQ indexes+32(FP), DX

    MOVQ values+48(FP), R8
    MOVQ size+64(FP), R9
    ADDQ offset+72(FP), R8

    XORQ SI, SI
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVQ (AX)(DI*8), DI
    MOVQ DI, (R8)
    ADDQ R9, R8
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    XORQ AX, AX
    MOVQ AX, ret+80(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, AX
    MOVQ AX, ret+80(FP)
    RET

// func dictionaryLookup32bitsAVX512(dict []uint32, indexes []int32, rows array, size, offset uintptr) errno
TEXT ·dictionaryLookup32bitsAVX512(SB), NOSPLIT, $0-88
    MOVQ dict+0(FP), AX
    MOVQ dict+8(FP), BX

    MOVQ indexes+24(FP), CX
    MOVQ indexes+32(FP), DX

    MOVQ values+48(FP), R8
    MOVQ size+64(FP), R9
    ADDQ offset+72(FP), R8

    XORQ SI, SI

    CMPQ DX, $8
    JB test

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    MOVQ R9, R10
    SHLQ $3, R10 // 8 * size

    MOVW $0xFFFF, R11
    KMOVW R11, K1
    KMOVW R11, K2

    VPBROADCASTD R9, Y2            // [size...]
    VPMULLD scale8x4<>(SB), Y2, Y2 // [0*size,1*size,...]
    VPBROADCASTD BX, Y3            // [len(dict)...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y3, Y0, K3
    KMOVW K3, R11
    CMPB R11, $0xFF
    JNE indexOutOfBounds
    VPGATHERDD (AX)(Y0*4), K1, Y1
    VPSCATTERDD Y1, K2, (R8)(Y2*1)
    KMOVW R11, K1
    KMOVW R11, K2
    ADDQ R10, R8
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512
    VZEROUPPER
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVL (AX)(DI*4), DI
    MOVL DI, (R8)
    ADDQ R9, R8
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    XORQ AX, AX
    MOVQ AX, ret+80(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, AX
    MOVQ AX, ret+80(FP)
    RET

// func dictionaryLookup64bitsAVX512(dict []uint64, indexes []int32, rows array, size, offset uintptr) errno
TEXT ·dictionaryLookup64bitsAVX512(SB), NOSPLIT, $0-88
    MOVQ dict+0(FP), AX
    MOVQ dict+8(FP), BX

    MOVQ indexes+24(FP), CX
    MOVQ indexes+32(FP), DX

    MOVQ values+48(FP), R8
    MOVQ size+64(FP), R9
    ADDQ offset+72(FP), R8

    XORQ SI, SI

    CMPQ DX, $8
    JB test

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    MOVQ R9, R10
    SHLQ $3, R10 // 8 * size

    MOVW $0xFFFF, R11
    KMOVW R11, K1
    KMOVW R11, K2

    VPBROADCASTD R9, Y2            // [size...]
    VPMULLD scale8x4<>(SB), Y2, Y2 // [0*size,1*size,...]
    VPBROADCASTD BX, Y3            // [len(dict)...]
loopAVX512:
    VMOVDQU32 (CX)(SI*4), Y0
    VPCMPUD $1, Y3, Y0, K3
    KMOVW K3, R11
    CMPB R11, $0xFF
    JNE indexOutOfBounds
    VPGATHERDQ (AX)(Y0*8), K1, Z1
    VPSCATTERDQ Z1, K2, (R8)(Y2*1)
    KMOVW R11, K1
    KMOVW R11, K2
    ADDQ R10, R8
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX512
    VZEROUPPER
    JMP test
loop:
    MOVL (CX)(SI*4), DI
    CMPL DI, BX
    JAE indexOutOfBounds
    MOVQ (AX)(DI*8), DI
    MOVQ DI, (R8)
    ADDQ R9, R8
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    XORQ AX, AX
    MOVQ AX, ret+80(FP)
    RET
indexOutOfBounds:
    MOVQ $errnoIndexOutOfBounds, AX
    MOVQ AX, ret+80(FP)
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
