//go:build !purego

#include "textflag.h"

#define blockSize 128
#define numMiniBlocks 4
#define miniBlockSize 32

#define deltaInt32AVX2x8(baseAddr) \
    VMOVDQU baseAddr, Y1    /* [0,1,2,3,4,5,6,7] */         \
    VPERMD Y1, Y3, Y2       /* [7,0,1,2,3,4,5,6] */         \
    VPBLENDD $1, Y0, Y2, Y2 /* [x,0,1,2,3,4,5,6] */         \
    VPSUBD Y2, Y1, Y2       /* [0,1,2,...] - [x,0,1,...] */ \
    VMOVDQU Y2, baseAddr                                    \
    VPERMD Y1, Y3, Y0

// func blockDeltaInt32AVX2(block *[blockSize]int32, lastValue int32) int32
TEXT ·blockDeltaInt32AVX2(SB), NOSPLIT, $0-24
    MOVQ block+0(FP), AX
    MOVL 4*blockSize-4(AX), CX
    MOVL CX, ret+16(FP)

    VPBROADCASTD lastValue+8(FP), Y0
    VMOVDQU deltaInt32Perm<>(SB), Y3

    deltaInt32AVX2x8(0(AX))
    deltaInt32AVX2x8(32(AX))
    deltaInt32AVX2x8(64(AX))
    deltaInt32AVX2x8(96(AX))
    deltaInt32AVX2x8(128(AX))
    deltaInt32AVX2x8(160(AX))
    deltaInt32AVX2x8(192(AX))
    deltaInt32AVX2x8(224(AX))
    deltaInt32AVX2x8(256(AX))
    deltaInt32AVX2x8(288(AX))
    deltaInt32AVX2x8(320(AX))
    deltaInt32AVX2x8(352(AX))
    deltaInt32AVX2x8(384(AX))
    deltaInt32AVX2x8(416(AX))
    deltaInt32AVX2x8(448(AX))
    deltaInt32AVX2x8(480(AX))
    VZEROUPPER
    RET

GLOBL deltaInt32Perm<>(SB), RODATA|NOPTR, $32
DATA deltaInt32Perm<>+0(SB)/4, $7
DATA deltaInt32Perm<>+4(SB)/4, $0
DATA deltaInt32Perm<>+8(SB)/4, $1
DATA deltaInt32Perm<>+12(SB)/4, $2
DATA deltaInt32Perm<>+16(SB)/4, $3
DATA deltaInt32Perm<>+20(SB)/4, $4
DATA deltaInt32Perm<>+24(SB)/4, $5
DATA deltaInt32Perm<>+28(SB)/4, $6

// func blockMinInt32AVX2(block *[blockSize]int32) int32
TEXT ·blockMinInt32AVX2(SB), NOSPLIT, $0-16
    MOVQ block+0(FP), AX
    VPBROADCASTD (AX), Y0
    VMOVDQU Y0, Y1
    VMOVDQU Y0, Y2
    VMOVDQU Y0, Y3
    VMOVDQU Y0, Y4
    VMOVDQU Y0, Y5
    VMOVDQU Y0, Y6
    VMOVDQU Y0, Y7
    VMOVDQU Y0, Y8
    VMOVDQU Y0, Y9
    VMOVDQU Y0, Y10
    VMOVDQU Y0, Y11
    VMOVDQU Y0, Y12
    VMOVDQU Y0, Y13
    VMOVDQU Y0, Y14
    VMOVDQU Y0, Y15

    VPMINSD 0(AX), Y0, Y0
    VPMINSD 32(AX), Y1, Y1
    VPMINSD 64(AX), Y2, Y2
    VPMINSD 96(AX), Y3, Y3
    VPMINSD 128(AX), Y4, Y4
    VPMINSD 160(AX), Y5, Y5
    VPMINSD 192(AX), Y6, Y6
    VPMINSD 224(AX), Y7, Y7
    VPMINSD 256(AX), Y8, Y8
    VPMINSD 288(AX), Y9, Y9
    VPMINSD 320(AX), Y10, Y10
    VPMINSD 352(AX), Y11, Y11
    VPMINSD 384(AX), Y12, Y12
    VPMINSD 416(AX), Y13, Y13
    VPMINSD 448(AX), Y14, Y14
    VPMINSD 480(AX), Y15, Y15

    VPMINSD Y1, Y0, Y0
    VPMINSD Y3, Y2, Y2
    VPMINSD Y5, Y4, Y4
    VPMINSD Y7, Y6, Y6
    VPMINSD Y9, Y8, Y8
    VPMINSD Y11, Y10, Y10
    VPMINSD Y13, Y12, Y12
    VPMINSD Y15, Y14, Y14

    VPMINSD Y2, Y0, Y0
    VPMINSD Y6, Y4, Y4
    VPMINSD Y10, Y8, Y8
    VPMINSD Y14, Y12, Y12

    VPMINSD Y4, Y0, Y0
    VPMINSD Y12, Y8, Y8

    VPMINSD Y8, Y0, Y0

    VPERM2I128 $1, Y0, Y0, Y1
    VPMINSD Y1, Y0, Y0

    VPSHUFD $0b00011011, Y0, Y1
    VPMINSD Y1, Y0, Y0
    VZEROUPPER

    MOVQ X0, CX
    MOVL CX, BX
    SHRQ $32, CX
    CMPL CX, BX
    CMOVLLT CX, BX
    MOVL BX, ret+8(FP)
    RET

#define subInt32AVX2x64(baseAddr, offset) \
    VMOVDQU offset+0(baseAddr), Y1      \
    VMOVDQU offset+32(baseAddr), Y2     \
    VMOVDQU offset+64(baseAddr), Y3     \
    VMOVDQU offset+96(baseAddr), Y4     \
    VMOVDQU offset+128(baseAddr), Y5    \
    VMOVDQU offset+160(baseAddr), Y6    \
    VMOVDQU offset+192(baseAddr), Y7    \
    VMOVDQU offset+224(baseAddr), Y8    \
    VPSUBD Y0, Y1, Y1                   \
    VPSUBD Y0, Y2, Y2                   \
    VPSUBD Y0, Y3, Y3                   \
    VPSUBD Y0, Y4, Y4                   \
    VPSUBD Y0, Y5, Y5                   \
    VPSUBD Y0, Y6, Y6                   \
    VPSUBD Y0, Y7, Y7                   \
    VPSUBD Y0, Y8, Y8                   \
    VMOVDQU Y1, offset+0(baseAddr)      \
    VMOVDQU Y2, offset+32(baseAddr)     \
    VMOVDQU Y3, offset+64(baseAddr)     \
    VMOVDQU Y4, offset+96(baseAddr)     \
    VMOVDQU Y5, offset+128(baseAddr)    \
    VMOVDQU Y6, offset+160(baseAddr)    \
    VMOVDQU Y7, offset+192(baseAddr)    \
    VMOVDQU Y8, offset+224(baseAddr)

// func blockSubInt32AVX2(block *[blockSize]int32, value int32)
TEXT ·blockSubInt32AVX2(SB), NOSPLIT, $0-16
    MOVQ block+0(FP), AX
    VPBROADCASTD value+8(FP), Y0
    subInt32AVX2x64(AX, 0)
    subInt32AVX2x64(AX, 256)
    VZEROUPPER
    RET

// func blockBitWidthsInt32AVX2(bitWidths *[numMiniBlocks]byte, block *[blockSize]int32)
TEXT ·blockBitWidthsInt32AVX2(SB), NOSPLIT, $0-16
    MOVQ bitWidths+0(FP), AX
    MOVQ block+8(FP), BX

    // AVX2 only has signed comparisons (and min/max), we emulate working on
    // unsigned values by adding -2^31 to the values. Y5 is a vector of -2^31
    // used to offset 8 packed 32 bits integers in other YMM registers where
    // the block data are loaded.
    VPCMPEQD Y5, Y5, Y5
    VPSLLD $31, Y5, Y5

    XORQ DI, DI
loop:
    VPBROADCASTD (BX), Y0 // max
    VPADDD Y5, Y0, Y0

    VMOVDQU (BX), Y1
    VMOVDQU 32(BX), Y2
    VMOVDQU 64(BX), Y3
    VMOVDQU 96(BX), Y4

    VPADDD Y5, Y1, Y1
    VPADDD Y5, Y2, Y2
    VPADDD Y5, Y3, Y3
    VPADDD Y5, Y4, Y4

    VPMAXSD Y2, Y1, Y1
    VPMAXSD Y4, Y3, Y3
    VPMAXSD Y3, Y1, Y1
    VPMAXSD Y1, Y0, Y0

    VPERM2I128 $1, Y0, Y0, Y1
    VPMAXSD Y1, Y0, Y0

    VPSHUFD $0b00011011, Y0, Y1
    VPMAXSD Y1, Y0, Y0
    VPSUBD Y5, Y0, Y0

    MOVQ X0, CX
    MOVL CX, DX
    SHRQ $32, CX
    CMPL CX, DX
    CMOVLHI CX, DX

    LZCNTL DX, DX
    NEGL DX
    ADDL $32, DX
    MOVB DX, (AX)(DI*1)

    ADDQ $128, BX
    INCQ DI
    CMPQ DI, $numMiniBlocks
    JNE loop
    VZEROUPPER
    RET

// miniBlockPackInt32Default is the generic implementation of the algorithm to
// pack 32 bit integers into values of a given bit width (<=32).
//
// This algorithm is much slower than the vectorized versions, but is useful
// as a reference implementation to run the tests against, and as fallback when
// the code runs on a CPU which does not support the AVX2 instruction set.
//
// func miniBlockPackInt32Default(dst *byte, src *[miniBlockSize]int32, bitWidth uint)
TEXT ·miniBlockPackInt32Default(SB), NOSPLIT, $0-24
    MOVQ dst+0(FP), AX
    MOVQ src+8(FP), BX
    MOVQ bitWidth+16(FP), R9

    XORQ DI, DI // bit offset
    XORQ SI, SI // mini block index
loop:
    MOVQ DI, CX
    MOVQ DI, DX

    ANDQ $0b11111, CX // i := bitOffset % 32
    SHRQ $5, DX       // j := bitOffset / 32

    MOVLQZX (BX)(SI*4), R8
    SHLQ CX, R8
    ORQ R8, (AX)(DX*4)

    ADDQ R9, DI
    INCQ SI
    CMPQ SI, $miniBlockSize
    JNE loop
    RET

// miniBlockPackInt32x1bitAVX2 packs 32 bit integers into 1 bit values in the
// the output buffer.
//
// The algorithm use MOVMSKPS to extract the 8 relevant bits from the 8 values
// packed in YMM registers, then combines 4 of these into  32 bit word which
// then gets written to the output. The result is 32 bits because each mini
// block has 32 values (the block size is 128 and there are 4 mini block per
// block).
//
// func miniBlockPackInt32x1bitAVX2(dst *byte, src *[miniBlockSize]int32)
TEXT ·miniBlockPackInt32x1bitAVX2(SB), NOSPLIT, $0-16
    MOVQ dst+0(FP), AX
    MOVQ src+8(FP), BX

    VMOVDQU 0(BX), Y0
    VMOVDQU 32(BX), Y1
    VMOVDQU 64(BX), Y2
    VMOVDQU 96(BX), Y3

    VPSLLD $31, Y0, Y0
    VPSLLD $31, Y1, Y1
    VPSLLD $31, Y2, Y2
    VPSLLD $31, Y3, Y3

    VMOVMSKPS Y0, R8
    VMOVMSKPS Y1, R9
    VMOVMSKPS Y2, R10
    VMOVMSKPS Y3, R11

    SHLL $8, R9
    SHLL $16, R10
    SHLL $24, R11

    ORL R9, R8
    ORL R10, R8
    ORL R11, R8
    MOVL R8, (AX)
    VZEROUPPER
    RET

// miniBlockPackInt32x2bitsAVX2 implements an algorithm for packing 32 bit
// integers into 2 bit values.
//
// The algorithm is derived from the one employed in miniBlockPackInt32x1bitAVX2
// but needs to perform a bit extra work since MOVMSKPS can only extract one bit
// per packed integer of each YMM vector. We run two passes to extract the two
// bits needed to compose each item of the result, and merge the values by
// interleaving the first and second bits with PDEP.
//
// func miniBlockPackInt32x2bitsAVX2(dst *byte, src *[miniBlockSize]int32)
TEXT ·miniBlockPackInt32x2bitsAVX2(SB), NOSPLIT, $0-16
    MOVQ dst+0(FP), AX
    MOVQ src+8(FP), BX

    VMOVDQU 0(BX), Y0
    VMOVDQU 32(BX), Y1
    VMOVDQU 64(BX), Y2
    VMOVDQU 96(BX), Y3

    VPSLLD $31, Y0, Y4
    VPSLLD $31, Y1, Y5
    VPSLLD $31, Y2, Y6
    VPSLLD $31, Y3, Y7

    VMOVMSKPS Y4, R8
    VMOVMSKPS Y5, R9
    VMOVMSKPS Y6, R10
    VMOVMSKPS Y7, R11

    SHLQ $8, R9
    SHLQ $16, R10
    SHLQ $24, R11
    ORQ R9, R8
    ORQ R10, R8
    ORQ R11, R8

    MOVQ $0x5555555555555555, DX // 0b010101...
    PDEPQ DX, R8, R8

    VPSLLD $30, Y0, Y8
    VPSLLD $30, Y1, Y9
    VPSLLD $30, Y2, Y10
    VPSLLD $30, Y3, Y11

    VMOVMSKPS Y8, R12
    VMOVMSKPS Y9, R13
    VMOVMSKPS Y10, R14
    VMOVMSKPS Y11, R15

    SHLQ $8, R13
    SHLQ $16, R14
    SHLQ $24, R15
    ORQ R13, R12
    ORQ R14, R12
    ORQ R15, R12

    MOVQ $0xAAAAAAAAAAAAAAAA, DI // 0b101010...
    PDEPQ DI, R12, R12

    ORQ R12, R8
    MOVQ R8, (AX)
    VZEROUPPER
    RET

// miniBlockPackInt32x3to16bitsAVX2 is the algorithm used to bit-pack 32 bit
// integers into values of width 3 to 16 bits.
//
// This function is a small overhead due to having to initialize registers with
// values that depend on the bit width. We measured this cost at ~10% throughput
// in synthetic benchmarks compared to generating constants shifts and offsets
// using a macro. Using a single function rather than generating one for each
// bit width has the benefit of reducing the code size, which in practice can
// also yield benefits like reducing CPU cache misses. Not using a macro also
// has other advantages like providing accurate line number of stack traces and
// enabling the use of breakpoints when debugging. Overall, this approach seemed
// to be the right trade off between performance and maintainability.
//
// The algorithm treats chunks of 8 values in 4 iterations to process all 32
// values of the mini block. Writes to the output buffer are aligned on 128 bits
// since we may write up to 128 bits (8 x 16 bits). Padding is therefore
// required in the output buffer to avoid triggering a segfault.
// The encodeInt32AVX2 method adds enough padding when sizing the output buffer
// to account of this requirement.
//
// We leverage the two lanes fo YMM registers to work on two sets of 4 values
// (in the sequence of VMOVDQU/VPSHUFD, VPAND, VPSLLQ, VPOR), resulting in having
// two sets of bit-packed values in the lower 64 bits of each YMM lane.
// The upper lane is then permuted into a lower lane to merge the two results,
// which may not be aligned on byte boundaries so we shift the lower and upper
// bits and compose two sets of 128 bits sequences (VPSLLQ, VPSRLQ, VBLENDPD),
// merge them and write the 16 bytes result to the output buffer.
TEXT ·miniBlockPackInt32x3to16bitsAVX2(SB), NOSPLIT, $0-24
    MOVQ dst+0(FP), AX
    MOVQ src+8(FP), BX

    VPBROADCASTQ bitWidths+16(FP), Y6 // [1*bitWidth...]
    VPSLLQ $1, Y6, Y7                 // [2*bitWidth...]
    VPADDQ Y6, Y7, Y8                 // [3*bitWidth...]
    VPSLLQ $2, Y6, Y9                 // [4*bitWidth...]

    VPBROADCASTQ sixtyfour<>(SB), Y10
    VPSUBQ Y6, Y10, Y11 // [64-1*bitWidth...]
    VPSUBQ Y9, Y10, Y12 // [64-4*bitWidth...]
    VPCMPEQQ Y4, Y4, Y4
    VPSRLVQ Y11, Y4, Y4

    VPXOR Y5, Y5, Y5
    XORQ SI, SI
loop:
    VMOVDQU (BX)(SI*4), Y0
    VPSHUFD $0b01010101, Y0, Y1
    VPSHUFD $0b10101010, Y0, Y2
    VPSHUFD $0b11111111, Y0, Y3

    VPAND Y4, Y0, Y0
    VPAND Y4, Y1, Y1
    VPAND Y4, Y2, Y2
    VPAND Y4, Y3, Y3

    VPSLLVQ Y6, Y1, Y1
    VPSLLVQ Y7, Y2, Y2
    VPSLLVQ Y8, Y3, Y3

    VPOR Y1, Y0, Y0
    VPOR Y3, Y2, Y2
    VPOR Y2, Y0, Y0

    VPERMQ $0b00001010, Y0, Y1

    VPSLLVQ X9, X1, X2
    VPSRLQ X12, X1, X3
    VBLENDPD $0b10, X3, X2, X1
    VBLENDPD $0b10, X5, X0, X0
    VPOR X1, X0, X0

    VMOVDQU X0, (AX)

    ADDQ CX, AX
    ADDQ $8, SI
    CMPQ SI, $miniBlockSize
    JNE loop
    VZEROUPPER
    RET

GLOBL sixtyfour<>(SB), RODATA|NOPTR, $32
DATA sixtyfour<>+0(SB)/8, $64
DATA sixtyfour<>+8(SB)/8, $64
DATA sixtyfour<>+16(SB)/8, $64
DATA sixtyfour<>+24(SB)/8, $64

// miniBlockPackInt32x32bitsAVX2 is a specialization of the bit packing logic
// for 32 bit integers when the output bit width is also 32, in which case a
// simple copy of the mini block to the output buffer produces the result.
//
// func miniBlockPackInt32x32bitsAVX2(dst *byte, src *[miniBlockSize]int32)
TEXT ·miniBlockPackInt32x32bitsAVX2(SB), NOSPLIT, $0-16
    MOVQ dst+0(FP), AX
    MOVQ src+8(FP), BX
    VMOVDQU 0(BX), Y0
    VMOVDQU 32(BX), Y1
    VMOVDQU 64(BX), Y2
    VMOVDQU 96(BX), Y3
    VMOVDQU Y0, 0(AX)
    VMOVDQU Y1, 32(AX)
    VMOVDQU Y2, 64(AX)
    VMOVDQU Y3, 96(AX)
    VZEROUPPER
    RET
