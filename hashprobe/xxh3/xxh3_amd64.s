//go:build !purego

#include "textflag.h"

#define key008 0x1cad21f72c81017c
#define key016 0xdb979083e96dd4de

// func multiHash32AVX512(hashes []uintptr, values []uint32, seed uintptr)
TEXT ·multiHash32AVX512(SB), NOSPLIT, $0-56
    MOVQ hashes_base+0(FP), AX
    MOVQ values_base+24(FP), BX
    MOVQ values_len+32(FP), CX
    VPBROADCASTQ seed+48(FP), Z0

    MOVW $0b0101010101010101, DX
    KMOVW DX, K1

    MOVQ $key008^key016, R8
    VPBROADCASTQ R8, Z4

    MOVQ $0x9fb21c651e98df25, R9
    VPBROADCASTQ R9, Z5

    MOVQ $4, R10
    VPBROADCASTQ R10, Z6

    XORQ SI, SI
loop:
    VPXORQ Z1, Z1, Z1
    VPEXPANDD (BX)(SI*4), K1, Z1
    VPSLLQ $32, Z1, Z2
    VPADDQ Z2, Z1, Z1  // hash = uint64(value) + (uint64(value) << 32)
    VPXORQ Z4, Z1, Z1  // hash ^= key008 ^ key016
    VPROLQ $49, Z1, Z2 // x = bits.RotateLeft64(hash, 49)
    VPROLQ $24, Z1, Z3 // y = bits.RotateLeft64(hash, 24)
    VPXORQ Z3, Z2, Z2  // x ^= y
    VPXORQ Z2, Z1, Z1  // hash ^= x
    VPMULLQ Z5, Z1, Z1 // hash *= 0x9fb21c651e98df25
    VPSRLQ $35, Z1, Z2 // x = hash >> 32
    VPADDQ Z6, Z2, Z2  // x += 4
    VPXORQ Z2, Z1, Z1  // hash ^= x
    VPMULLQ Z5, Z1, Z1 // hash *= 0x9fb21c651e98df25
    VPSRLQ $28, Z1, Z2 // x = hash >> 28
    VPXORQ Z2, Z1, Z1  // hash ^= x
    VMOVDQU64 Z1, (AX)(SI*8)
    ADDQ $8, SI
    CMPQ SI, CX
    JNE loop
    RET
