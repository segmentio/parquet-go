//go:build !purego

#include "textflag.h"

#define blockSize 128
#define numMiniBlocks 4
#define miniBlockSize 32

#define deltaInt32x8(baseAddr, lastValue, offset) \
    MOVL offset+0(AX), R8        \
    MOVL offset+4(AX), R9        \
    MOVL offset+8(AX), R10       \
    MOVL offset+12(AX), R11      \
    MOVL offset+16(AX), R12      \
    MOVL offset+20(AX), R13      \
    MOVL offset+24(AX), R14      \
    MOVL offset+28(AX), R15      \
    SUBL lastValue, offset+0(AX) \
    SUBL R8, offset+4(AX)        \
    SUBL R9, offset+8(AX)        \
    SUBL R10, offset+12(AX)      \
    SUBL R11, offset+16(AX)      \
    SUBL R12, offset+20(AX)      \
    SUBL R13, offset+24(AX)      \
    SUBL R14, offset+28(AX)      \
    MOVL R15, lastValue

// func blockDeltaInt32(block *[blockSize]int32, lastValue int32) int32
TEXT ·blockDeltaInt32(SB), NOSPLIT, $0-24
    MOVQ block+0(FP), AX
    MOVL lastValue+8(FP), BX
    deltaInt32x8(AX, BX, 0)
    deltaInt32x8(AX, BX, 32)
    deltaInt32x8(AX, BX, 64)
    deltaInt32x8(AX, BX, 96)
    deltaInt32x8(AX, BX, 128)
    deltaInt32x8(AX, BX, 160)
    deltaInt32x8(AX, BX, 192)
    deltaInt32x8(AX, BX, 224)
    deltaInt32x8(AX, BX, 256)
    deltaInt32x8(AX, BX, 288)
    deltaInt32x8(AX, BX, 320)
    deltaInt32x8(AX, BX, 352)
    deltaInt32x8(AX, BX, 384)
    deltaInt32x8(AX, BX, 416)
    deltaInt32x8(AX, BX, 448)
    deltaInt32x8(AX, BX, 480)
    MOVL BX, ret+16(FP)
    RET

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
    VMOVDQU blockDeltaInt32Perm<>(SB), Y3

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

GLOBL blockDeltaInt32Perm<>(SB), RODATA|NOPTR, $32
DATA blockDeltaInt32Perm<>+0(SB)/4, $7
DATA blockDeltaInt32Perm<>+4(SB)/4, $0
DATA blockDeltaInt32Perm<>+8(SB)/4, $1
DATA blockDeltaInt32Perm<>+12(SB)/4, $2
DATA blockDeltaInt32Perm<>+16(SB)/4, $3
DATA blockDeltaInt32Perm<>+20(SB)/4, $4
DATA blockDeltaInt32Perm<>+24(SB)/4, $5
DATA blockDeltaInt32Perm<>+28(SB)/4, $6

#define minInt32x8(baseAddr, min, offset) \
    MOVL offset+0(baseAddr), R8   \
    MOVL offset+4(baseAddr), R9   \
    MOVL offset+8(baseAddr), R10  \
    MOVL offset+12(baseAddr), R11 \
    MOVL offset+16(baseAddr), R12 \
    MOVL offset+20(baseAddr), R13 \
    MOVL offset+24(baseAddr), R14 \
    MOVL offset+28(baseAddr), R15 \
                                  \
    CMPL R9, R8                   \
    CMOVLLT R9, R8                \
                                  \
    CMPL R11, R10                 \
    CMOVLLT R11, R10              \
                                  \
    CMPL R13, R12                 \
    CMOVLLT R13, R12              \
                                  \
    CMPL R15, R14                 \
    CMOVLLT R15, R14              \
                                  \
    CMPL R10, R8                  \
    CMOVLLT R10, R8               \
                                  \
    CMPL R14, R12                 \
    CMOVLLT R14, R12              \
                                  \
    CMPL R12, R8                  \
    CMOVLLT R12, R8               \
                                  \
    CMPL R8, min                  \
    CMOVLLT R8, min

// func blockMinInt32(block *[blockSize]int32) int32
TEXT ·blockMinInt32(SB), NOSPLIT, $0-16
    MOVQ block+0(FP), AX
    MOVL (AX), BX
    minInt32x8(AX, BX, 0)
    minInt32x8(AX, BX, 32)
    minInt32x8(AX, BX, 64)
    minInt32x8(AX, BX, 96)
    minInt32x8(AX, BX, 128)
    minInt32x8(AX, BX, 160)
    minInt32x8(AX, BX, 192)
    minInt32x8(AX, BX, 224)
    minInt32x8(AX, BX, 256)
    minInt32x8(AX, BX, 288)
    minInt32x8(AX, BX, 320)
    minInt32x8(AX, BX, 352)
    minInt32x8(AX, BX, 384)
    minInt32x8(AX, BX, 416)
    minInt32x8(AX, BX, 448)
    minInt32x8(AX, BX, 480)
    MOVL BX, ret+8(FP)
    RET

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

#define subInt32x8(baseAddr, value, offset) \
    SUBL value, offset+0(baseAddr)          \
    SUBL value, offset+4(baseAddr)          \
    SUBL value, offset+8(baseAddr)          \
    SUBL value, offset+12(baseAddr)         \
    SUBL value, offset+16(baseAddr)         \
    SUBL value, offset+20(baseAddr)         \
    SUBL value, offset+24(baseAddr)         \
    SUBL value, offset+28(baseAddr)

// func blockSubInt32(block *[blockSize]int32, value int32)
TEXT ·blockSubInt32(SB), NOSPLIT, $0-16
    MOVQ block+0(FP), AX
    MOVQ value+8(FP), BX
    subInt32x8(AX, BX, 0)
    subInt32x8(AX, BX, 32)
    subInt32x8(AX, BX, 64)
    subInt32x8(AX, BX, 96)
    subInt32x8(AX, BX, 128)
    subInt32x8(AX, BX, 160)
    subInt32x8(AX, BX, 192)
    subInt32x8(AX, BX, 224)
    subInt32x8(AX, BX, 256)
    subInt32x8(AX, BX, 288)
    subInt32x8(AX, BX, 320)
    subInt32x8(AX, BX, 352)
    subInt32x8(AX, BX, 384)
    subInt32x8(AX, BX, 416)
    subInt32x8(AX, BX, 448)
    subInt32x8(AX, BX, 480)
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

#define blockBitWidthsInt32x8(baseAddr, dst, offset) \
    MOVL offset+0(baseAddr), R8   \
    MOVL offset+4(baseAddr), R9   \
    MOVL offset+8(baseAddr), R10  \
    MOVL offset+12(baseAddr), R11 \
    MOVL offset+16(baseAddr), R12 \
    MOVL offset+20(baseAddr), R13 \
    MOVL offset+24(baseAddr), R14 \
    MOVL offset+28(baseAddr), R15 \
                                  \
    CMPL R9, R8                   \
    CMOVLHI R9, R8                \
                                  \
    CMPL R11, R10                 \
    CMOVLHI R11, R10              \
                                  \
    CMPL R13, R12                 \
    CMOVLHI R13, R12              \
                                  \
    CMPL R15, R14                 \
    CMOVLHI R15, R14              \
                                  \
    CMPL R10, R8                  \
    CMOVLHI R10, R8               \
                                  \
    CMPL R14, R12                 \
    CMOVLHI R14, R12              \
                                  \
    CMPL R12, R8                  \
    CMOVLHI R12, R8               \
                                  \
    CMPL R8, dst                  \
    CMOVLHI R8, dst

#define blockBitWidthsInt32x32(src, dst, offset) \
    XORQ dst, dst                                    \
    blockBitWidthsInt32x8(src, dst, offset+0)    \
    blockBitWidthsInt32x8(src, dst, offset+32)   \
    blockBitWidthsInt32x8(src, dst, offset+64)   \
    blockBitWidthsInt32x8(src, dst, offset+96)   \
    LZCNTL dst, dst                                  \
    NEGL dst                                         \
    ADDL $32, dst

// func blockBitWidthsInt32(bitWidths *[numMiniBlocks]byte, block *[blockSize]int32)
TEXT ·blockBitWidthsInt32(SB), NOSPLIT, $0-16
    MOVQ block+8(FP), DI
    blockBitWidthsInt32x32(DI, AX, 0)
    blockBitWidthsInt32x32(DI, BX, 128)
    blockBitWidthsInt32x32(DI, CX, 256)
    blockBitWidthsInt32x32(DI, DX, 384)
    MOVQ bitWidths+0(FP), DI
    MOVB AX, 0(DI)
    MOVB BX, 1(DI)
    MOVB CX, 2(DI)
    MOVB DX, 3(DI)
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

// miniBlockCopyInt32 is the generic implementation of the algorithm to pack
// 32 bit integers into values of a given bit width (<=32).
//
// This algorithm is much slower than the vectorized versions, but is useful
// as a reference implementation to run the tests against, and as fallback when
// the code runs on a CPU which does not support the AVX2 instruction set.
//
// func miniBlockCopyInt32(dst *byte, src *[miniBlockSize]int32, bitWidth uint)
TEXT ·miniBlockCopyInt32(SB), NOSPLIT, $0-24
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

// miniBlockCopyInt32x1bitAVX2 packs 32 bit integers into 1 bit values in the
// the output buffer.
//
// The algorithm use MOVMSKPS to extract the 8 relevant bits from the 8 values
// packed in YMM registers, then combines 4 of these into  32 bit word which
// then gets written to the output. The result is 32 bits because each mini
// block has 32 values (the block size is 128 and there are 4 mini block per
// block).
//
// func miniBlockCopyInt32x1bitAVX2(dst *byte, src *[miniBlockSize]int32)
TEXT ·miniBlockCopyInt32x1bitAVX2(SB), NOSPLIT, $0-16
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

// miniBlockCopyInt32x2bitsAVX2 implements an algorithm for packing 32 bit
// integers into 2 bit values.
//
// The algorithm is derived from the one employed in miniBlockCopyInt32x1bitAVX2
// but needs to perform a bit extra work since MOVMSKPS can only extract one bit
// per packed integer of each YMM vector. We run two passes to extract the two
// bits needed to compose each item of the result, and merge the values by
// interleaving the first and second bits with PDEP.
//
// func miniBlockCopyInt32x2bitsAVX2(dst *byte, src *[miniBlockSize]int32)
TEXT ·miniBlockCopyInt32x2bitsAVX2(SB), NOSPLIT, $0-16
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

// The miniBlockCopyInt32x3to8bitsAVX2 macro is used to generate the code for
// functions packing 32 bit integers into values of width 3 to 8 bits.
//
// The use of a macro helps generate constant offsets which are scaled off of
// the bit packing width.
//
// The algorithm treats chunks of 8 values in 4 iterations to process all 32
// values of the mini block. Writes to the output buffer are aligned on 64 bits
// since we may write up to 64 bits. Padding is therefore required in the output
// buffer to avoid triggering a segfault. The encodeInt32AVX2 method adds enough
// padding when sizing the output buffer to account of this requirement.
#define miniBlockCopyInt32x3to8bitsAVX2(bitWidth) \
    MOVQ dst+0(FP), AX              \
    MOVQ src+8(FP), BX              \
                                    \
    XORQ DI, DI                     \
    NOTQ DI                         \
    SHRQ $64-4*bitWidth, DI         \
                                    \
    XORQ SI, SI                     \
loop:                               \
    VMOVDQU (BX)(SI*4), Y0          \
    VPSHUFD $0b01010101, Y0, Y1     \
    VPSHUFD $0b10101010, Y0, Y2     \
    VPSHUFD $0b11111111, Y0, Y3     \
                                    \
    VPSLLD $1*bitWidth, Y1, Y1      \
    VPSLLD $2*bitWidth, Y2, Y2      \
    VPSLLD $3*bitWidth, Y3, Y3      \
                                    \
    VPOR Y1, Y0, Y0                 \
    VPOR Y3, Y2, Y2                 \
    VPOR Y2, Y0, Y0                 \
                                    \
    VPERM2I128 $1, Y0, Y0, Y1       \
                                    \
    MOVQ X0, R8                     \
    MOVQ X1, R9                     \
                                    \
    ANDQ DI, R8                     \
    ANDQ DI, R9                     \
                                    \
    SHLQ $4*bitWidth, R9            \
    ORQ R9, R8                      \
    MOVQ R8, (AX)                   \
                                    \
    ADDQ $bitWidth, AX              \
    ADDQ $8, SI                     \
    CMPQ SI, $miniBlockSize         \
    JNE loop                        \
    VZEROUPPER                      \
    RET

TEXT ·miniBlockCopyInt32x3bitsAVX2(SB), NOSPLIT, $0-16
    miniBlockCopyInt32x3to8bitsAVX2(3)

TEXT ·miniBlockCopyInt32x4bitsAVX2(SB), NOSPLIT, $0-16
    miniBlockCopyInt32x3to8bitsAVX2(4)

TEXT ·miniBlockCopyInt32x5bitsAVX2(SB), NOSPLIT, $0-16
    miniBlockCopyInt32x3to8bitsAVX2(5)

TEXT ·miniBlockCopyInt32x6bitsAVX2(SB), NOSPLIT, $0-16
    miniBlockCopyInt32x3to8bitsAVX2(6)

TEXT ·miniBlockCopyInt32x7bitsAVX2(SB), NOSPLIT, $0-16
    miniBlockCopyInt32x3to8bitsAVX2(7)

TEXT ·miniBlockCopyInt32x8bitsAVX2(SB), NOSPLIT, $0-16
    miniBlockCopyInt32x3to8bitsAVX2(8)

// miniBlockCopyInt32x32bitsAVX2 is a specialization of the bit packing logic
// for 32 bit integers when the output bit width is also 32, in which case a
// simple copy of the mini block to the output buffer produces the result.
//
// func miniBlockCopyInt32x32bitsAVX2(dst *byte, src *[miniBlockSize]int32)
TEXT ·miniBlockCopyInt32x32bitsAVX2(SB), NOSPLIT, $0-16
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

/*
TEXT ·miniBlockCopyInt32x2to7bitsAVX2(SB), NOSPLIT, $0-24
    MOVQ dst+0(FP), AX
    MOVQ src+8(FP), BX
    MOVQ bitWidth+16(FP), CX // bitWidth

    MOVQ CX, DX // (8*bitWidth)/8 ~= bitWidth
    SHLQ $2, CX // bitWidth *= 4; we work on 4 values in each register lane
    MOVQ $1, DI // bitMask := (1 << bitWidth) - 1
    SHLQ CX, DI
    DECQ DI

    VPBROADCASTD bitWidth+16(FP), Y4
    VPSLLD $1, Y4, Y8  // 2*bitWidth
    VMOVDQU Y4, Y5    // [1*bitWidth...]
    VPADDD Y4, Y4, Y6 // [2*bitWidth...]
    VPADDD Y8, Y4, Y7 // [3*bitWidth...]

    XORQ SI, SI
loop:
    VMOVDQU (BX)(SI*4), Y0
    VPSHUFD $0b00111001, Y0, Y1
    VPSHUFD $0b01001110, Y0, Y2
    VPSHUFD $0b10010011, Y0, Y3

    VPSLLVD Y5, Y1, Y1
    VPSLLVD Y6, Y2, Y2
    VPSLLVD Y7, Y3, Y3

    VPOR Y1, Y0, Y0
    VPOR Y3, Y2, Y2
    VPOR Y2, Y0, Y0

    VPERM2I128 $1, Y0, Y0, Y1

    MOVQ X0, R8
    MOVQ X1, R9

    ANDQ DI, R8
    ANDQ DI, R9

    SHLQ CX, R9
    ORQ R9, R8
    MOVQ R8, (AX)

    ADDQ DX, AX
    ADDQ $8, SI
    CMPQ SI, $miniBlockSize
    JNE loop

    VZEROUPPER
    RET
*/
