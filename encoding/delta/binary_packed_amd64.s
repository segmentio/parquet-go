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

#define cmpInt32x8(baseAddr, dst, offset, CMOV) \
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
    CMOV R9, R8                   \
                                  \
    CMPL R11, R10                 \
    CMOV R11, R10                 \
                                  \
    CMPL R13, R12                 \
    CMOV R13, R12                 \
                                  \
    CMPL R15, R14                 \
    CMOV R15, R14                 \
                                  \
    CMPL R10, R8                  \
    CMOV R10, R8                  \
                                  \
    CMPL R14, R12                 \
    CMOV R14, R12                 \
                                  \
    CMPL R12, R8                  \
    CMOV R12, R8                  \
                                  \
    CMPL R8, dst                  \
    CMOV R8, dst

#define minInt32x8(baseAddr, min, offset) \
    cmpInt32x8(baseAddr, min, offset, CMOVLLT)

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

#define miniBlockBitWidthsInt32x8(src, dst, offset) \
    cmpInt32x8(src, dst, offset, CMOVLHI)

#define miniBlockBitWidthsInt32x32(src, dst, offset) \
    miniBlockBitWidthsInt32x8(src, dst, offset+0)  \
    miniBlockBitWidthsInt32x8(src, dst, offset+32) \
    miniBlockBitWidthsInt32x8(src, dst, offset+64) \
    miniBlockBitWidthsInt32x8(src, dst, offset+96) \
    LZCNTL dst, R8                                 \
    MOVL $32, dst                                  \
    SUBL R8, dst

// func miniBlockBitWidthsInt32(bitWidths *[numMiniBlocks]byte, block *[blockSize]int32)
TEXT ·miniBlockBitWidthsInt32(SB), NOSPLIT, $0-16
    MOVQ block+8(FP), DI
    XORQ AX, AX
    XORQ BX, BX
    XORQ CX, CX
    XORQ DX, DX
    miniBlockBitWidthsInt32x32(DI, AX, 0)
    miniBlockBitWidthsInt32x32(DI, BX, 128)
    miniBlockBitWidthsInt32x32(DI, CX, 256)
    miniBlockBitWidthsInt32x32(DI, DX, 384)
    MOVQ bitWidths+0(FP), DI
    MOVB AX, 0(DI)
    MOVB BX, 1(DI)
    MOVB CX, 2(DI)
    MOVB DX, 3(DI)
    RET
