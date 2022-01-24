//go:build !purego

#include "textflag.h"

DATA salt_even<>+0(SB)/8, $0x47b6137b
DATA salt_even<>+8(SB)/8, $0x8824ad5b
DATA salt_even<>+16(SB)/8, $0x705495c7
DATA salt_even<>+24(SB)/8, $0x9efc4947
GLOBL salt_even<>(SB), RODATA|NOPTR, $32

DATA salt_odd<>+0(SB)/8, $0x44974d91
DATA salt_odd<>+8(SB)/8, $0xa2b7289d
DATA salt_odd<>+16(SB)/8, $0x2df1424b
DATA salt_odd<>+24(SB)/8, $0x5c6bfb31
GLOBL salt_odd<>(SB), RODATA|NOPTR, $32

DATA shift_odd<>+0(SB)/8, $32
DATA shift_odd<>+8(SB)/8, $32
DATA shift_odd<>+16(SB)/8, $32
DATA shift_odd<>+24(SB)/8, $32
GLOBL shift_odd<>(SB), RODATA|NOPTR, $32

DATA ones<>+0(SB)/4, $1
DATA ones<>+4(SB)/4, $1
DATA ones<>+8(SB)/4, $1
DATA ones<>+12(SB)/4, $1
DATA ones<>+16(SB)/4, $1
DATA ones<>+20(SB)/4, $1
DATA ones<>+24(SB)/4, $1
DATA ones<>+28(SB)/4, $1
GLOBL ones<>(SB), RODATA|NOPTR, $32

// This initial block is a SIMD implementation of the mask function declared in
// block_default.go and block_optimized.go, it is repeated in block_check as
// well. For each of the 8 x 32 bits words of the bloom filter block, the
// operation performed is:
//
//      block[i] = 1 << ((x * salt[i]) >> 27)
//
// The algorithm works on the even and odd indexes of the array of salt values
// because the VPMULUDQ instruction used to perform multiplication of the 32
// bits input by the salts produces 4 x 64 bits results in each quad word of
// the destination YMM register. Since there are no dependencies between the
// odd and even parts (until we merge the results), the cost of the extra
// instructions can be amortized by CPU pipelining allowing those to be
// processed in parallel.
//
// After computing the multiplications, the 4 quad words of the YMM register
// holding the odd indexes is shifted left by 32 bits, and the registers for
// the even and odd indexes are blended to produce the combined results of all
// the multiplications.
//
// An alternative could be to use VPMULLD which keeps only the lower 32 bits of
// the multiplication; however, it appeared to yield worse performance on the
// benchmarks we ran.
#define generateMask \
    VPBROADCASTD x+8(FP), Y0 \
    VPBROADCASTD x+8(FP), Y1 \
    VMOVDQA ones<>(SB), Y2 \
    VPMULUDQ salt_even<>+0(SB), Y0, Y0 \
    VPMULUDQ salt_odd<>+0(SB), Y1, Y1 \
    VPSLLVQ shift_odd<>+0(SB), Y1, Y1 \
    VBLENDPS $0b01010101, Y0, Y1, Y1 \
    VPSRLD $27, Y1, Y1 \
    VPSLLVD Y1, Y2, Y2

// func block_insert(b *Block, x uint32)
// Requires: AVX, AVX2
TEXT ·block_insert(SB), NOSPLIT, $0-16
    MOVQ b+0(FP), AX
    generateMask
    // Set all 1 bits of the mask in the bloom filter block.
    VPOR (AX), Y2, Y2
    VMOVUPS Y2, (AX)
    VZEROUPPER
    RET

// func block_check(b *Block, x uint32) bool
// Requires: AVX, AVX2
TEXT ·block_check(SB), NOSPLIT, $0-17
    MOVQ b+0(FP), AX
    generateMask
    // Compare the 1 bits of the mask with the bloom filter block, then compare
    // the result with the mask, expecting equality if the value `x` was present
    // in the block.
    VPAND (AX), Y2, Y0 // Y0 = block & mask
    VPTEST Y2, Y0      // if (Y2 & ^Y0) != 0 { CF = 1 }
    SETCS ret+16(FP)   // return CF == 1
    VZEROUPPER
    RET
