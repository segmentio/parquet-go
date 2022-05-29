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

TEXT ·miniBlockCopyInt32x8bitsAVX2(SB), NOSPLIT, $0-16
    MOVQ dst+0(FP), AX
    MOVQ src+8(FP), BX

    XORQ SI, SI
loop:
    VMOVDQU (BX)(SI*4), X0
    VPSHUFD $0b00111001, X0, X1
    VPSHUFD $0b01001110, X0, X2
    VPSHUFD $0b10010011, X0, X3
    VPSLLD $8, X1, X1
    VPSLLD $16, X2, X2
    VPSLLD $24, X3, X3
    VPOR X1, X0, X0
    VPOR X3, X2, X2
    VPOR X2, X0, X0

    //VPERM2I128 $1, Y0, Y0, Y1

    MOVQ X0, CX
    //MOVQ X1, DX
    //ANDQ $0xFFFFFFFF, CX
    //SHLQ $32, DX
    //ORQ DX, CX
    //MOVQ CX, (AX)
    MOVL CX, (AX)(SI*1)

    ADDQ $4, SI
    CMPQ SI, $miniBlockSize
    JNE loop

    VZEROUPPER
    RET

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
