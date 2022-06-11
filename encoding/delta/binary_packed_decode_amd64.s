//go:build !purego

#include "textflag.h"

// -----------------------------------------------------------------------------
// 32 bits
// -----------------------------------------------------------------------------

// func decodeBlockInt32Default(dst []int32, minDelta, lastValue int32) int32
TEXT ·decodeBlockInt32Default(SB), NOSPLIT, $0-36
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), BX
    MOVLQZX minDelta+24(FP), CX
    MOVLQZX lastValue+28(FP), DX
    XORQ SI, SI
    JMP test
loop:
    MOVL (AX)(SI*4), DI
    ADDL CX, DI
    ADDL DI, DX
    MOVL DX, (AX)(SI*4)
    INCQ SI
test:
    CMPQ SI, BX
    JNE loop
done:
    MOVL DX, ret+32(FP)
    RET

// func decodeBlockInt32AVX2(dst []int32, minDelta, lastValue int32) int32
TEXT ·decodeBlockInt32AVX2(SB), NOSPLIT, $0-36
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), BX
    MOVLQZX minDelta+24(FP), CX
    MOVLQZX lastValue+28(FP), DX
    XORQ SI, SI

    CMPQ BX, $8
    JB test

    MOVQ BX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    VPXOR X1, X1, X1
    MOVQ CX, X0
    MOVQ DX, X1
    VPBROADCASTD X0, Y0
loopAVX2:
    VMOVDQU (AX)(SI*4), Y2
    VPADDD Y0, Y2, Y2 // Y2[:] += minDelta
    VPADDD Y1, Y2, Y2 // Y2[0] += lastValue

    VPSLLDQ $4, Y2, Y3
    VPADDD Y3, Y2, Y2

    VPSLLDQ $8, Y2, Y3
    VPADDD Y3, Y2, Y2

    VPSHUFD $0xFF, X2, X1
    VPERM2I128 $1, Y2, Y2, Y3
    VPADDD X1, X3, X3

    VMOVDQU X2, (AX)(SI*4)
    VMOVDQU X3, 16(AX)(SI*4)
    VPSRLDQ $12, X3, X1 // lastValue

    ADDQ $8, SI
    CMPQ SI, DI
    JNE loopAVX2
    VZEROUPPER
    MOVQ X1, DX
    JMP test
loop:
    MOVL (AX)(SI*4), DI
    ADDL CX, DI
    ADDL DI, DX
    MOVL DX, (AX)(SI*4)
    INCQ SI
test:
    CMPQ SI, BX
    JNE loop
done:
    MOVL DX, ret+32(FP)
    RET

// func decodeMiniBlockInt32Default(dst []int32, src []uint32, bitWidth uint)
TEXT ·decodeMiniBlockInt32Default(SB), NOSPLIT, $0-56
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), DX
    MOVQ src_base+24(FP), BX
    MOVQ bitWidth+48(FP), CX

    MOVQ $1, R8 // bitMask = (1 << bitWidth) - 1
    SHLQ CX, R8
    DECQ R8
    MOVQ CX, R9 // bitWidth

    XORQ DI, DI // bitOffset
    XORQ SI, SI // index
    JMP test
loop:
    MOVQ DI, R10
    MOVQ DI, CX
    SHRQ $5, R10      // i = bitOffset / 32
    ANDQ $0b11111, CX // j = bitOffset % 32

    MOVL (BX)(R10*4), R11
    MOVL R8, R12  // d = bitMask
    SHLL CX, R12  // d = d << j
    ANDL R12, R11 // d = src[i] & d
    SHRL CX, R11  // d = d >> j

    MOVL CX, R13
    ADDL R9, R13
    CMPL R13, $32
    JBE next // j+bitWidth <= 32 ?

    MOVL 4(BX)(R10*4), R14
    MOVL CX, R12
    MOVL $32, CX
    SUBL R12, CX  // k = 32 - j
    MOVL R8, R12  // c = bitMask
    SHRL CX, R12  // c = c >> k
    ANDL R12, R14 // c = src[i+1] & c
    SHLL CX, R14  // c = c << k
    ORL R14, R11  // d = d | c
next:
    MOVL R11, (AX)(SI*4) // dst[n] = d
    ADDQ R9, DI          // bitOffset += bitWidth
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    RET

// func decodeMiniBlockInt32x1bit1AVX2(dst []int32, src []uint32)
TEXT ·decodeMiniBlockInt32x1bitAVX2(SB), NOSPLIT, $0-48
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), DX
    MOVQ src_base+24(FP), BX
    XORQ SI, SI

    CMPQ DX, $8
    JB test

    MOVQ DX, CX
    SHRQ $3, CX
    SHLQ $3, CX
    XORQ DI, DI
    MOVQ $0x0101010101010101, R8

    VPXOR X0, X0, X0
    MOVQ $4, R9
    MOVQ R9, X4
    VPBROADCASTD X4, X4
    VMOVDQU shuffleMask1bit<>(SB), X3
    VPADDD X3, X4, X4
loopAVX2:
    MOVB (BX)(DI*1), R9
    PDEPQ R8, R9, R9
    MOVQ R9, X0
    VPSHUFB X3, X0, X1
    VPSHUFB X4, X0, X2
    VMOVDQU X1, (AX)(SI*4)
    VMOVDQU X2, 16(AX)(SI*4)
    ADDQ $8, SI
    INCQ DI
    CMPQ SI, CX
    JNE loopAVX2
    JMP test
loop: // dst[i] = (src[i/32] >> (i%32)) & 1
    MOVQ SI, DI
    MOVQ SI, CX
    SHRQ $5, DI
    ANDQ $0b11111, CX
    MOVL (BX)(DI*4), DI
    SHRL CX, DI
    ANDL $1, DI
    MOVL DI, (AX)(SI*4)
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    RET

GLOBL shuffleMask1bit<>(SB), RODATA|NOPTR, $16
DATA shuffleMask1bit<>+0(SB)/4, $0x80808000
DATA shuffleMask1bit<>+4(SB)/4, $0x80808001
DATA shuffleMask1bit<>+8(SB)/4, $0x80808002
DATA shuffleMask1bit<>+12(SB)/4, $0x80808003

// func decodeMiniBlockInt32x8bitsAVX2(dst []int32, src []uint32)
TEXT ·decodeMiniBlockInt32x8bitsAVX2(SB), NOSPLIT, $0-48
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), DX
    MOVQ src_base+24(FP), BX
    XORQ SI, SI

    CMPQ DX, $32
    JB test

    MOVQ DX, CX
    SHRQ $5, CX
    SHLQ $5, CX
    XORQ DI, DI

    MOVQ $4, R8
    MOVQ R8, X4
    VPBROADCASTD X4, Y4

    VMOVDQU shuffleMask8bits<>(SB), Y6
    VPADDD Y4, Y6, Y7
    VPADDD Y4, Y7, Y8
    VPADDD Y4, Y8, Y9
loopAVX2:
    VMOVDQU (BX)(DI*4), Y5            // [0..15]  [16..31]

    VPSHUFB Y6, Y5, Y0                // [0..3]   [16..19]
    VPSHUFB Y7, Y5, Y1                // [4..7]   [20..23]
    VPSHUFB Y8, Y5, Y2                // [8..11]  [24..27]
    VPSHUFB Y9, Y5, Y3                // [12..15] [28..31]

    VPERM2I128 $0b100000, Y1, Y0, Y10 // [0..3]   [4..7]
    VPERM2I128 $0b100000, Y3, Y2, Y11 // [8..11]  [12..15]
    VPERM2I128 $0b110001, Y1, Y0, Y12 // [16..19] [20..23]
    VPERM2I128 $0b110001, Y3, Y2, Y13 // [24..27] [28..31]

    VMOVDQU Y10, (AX)(SI*4)
    VMOVDQU Y11, 32(AX)(SI*4)
    VMOVDQU Y12, 64(AX)(SI*4)
    VMOVDQU Y13, 96(AX)(SI*4)

    ADDQ $32, SI
    ADDQ $8, DI
    CMPQ SI, CX
    JNE loopAVX2
    VZEROUPPER
    JMP test
loop: // dst[i] = (src[i/4] >> (8 * (i%4))) & 0xFF
    MOVQ SI, DI
    MOVQ SI, CX
    SHRQ $2, DI
    ANDQ $0b11, CX
    SHLQ $3, CX
    MOVL (BX)(DI*4), DI
    SHRL CX, DI
    ANDL $0xFF, DI
    MOVL DI, (AX)(SI*4)
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    RET

GLOBL shuffleMask8bits<>(SB), RODATA|NOPTR, $32
DATA shuffleMask8bits<>+0(SB)/4, $0x80808000
DATA shuffleMask8bits<>+4(SB)/4, $0x80808001
DATA shuffleMask8bits<>+8(SB)/4, $0x80808002
DATA shuffleMask8bits<>+12(SB)/4, $0x80808003
DATA shuffleMask8bits<>+16(SB)/4, $0x80808000
DATA shuffleMask8bits<>+20(SB)/4, $0x80808001
DATA shuffleMask8bits<>+24(SB)/4, $0x80808002
DATA shuffleMask8bits<>+28(SB)/4, $0x80808003

// func decodeMiniBlockInt32x16bitsAVX2(dst []int32, src []uint32)
TEXT ·decodeMiniBlockInt32x16bitsAVX2(SB), NOSPLIT, $0-48
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), DX
    MOVQ src_base+24(FP), BX
    XORQ SI, SI

    CMPQ DX, $16
    JB test

    MOVQ DX, CX
    SHRQ $4, CX
    SHLQ $4, CX
    XORQ DI, DI

    MOVQ $0x0808, R8
    MOVQ R8, X4
    VPBROADCASTD X4, Y4

    VMOVDQU shuffleMask16bits<>(SB), Y6
    VPADDB Y4, Y6, Y7
loopAVX2:
    VMOVDQU (BX)(DI*4), Y5
    VPSHUFB Y6, Y5, Y0
    VPSHUFB Y7, Y5, Y1
    VPERM2I128 $0b100000, Y1, Y0, Y10
    VPERM2I128 $0b110001, Y1, Y0, Y11
    VMOVDQU Y10, (AX)(SI*4)
    VMOVDQU Y11, 32(AX)(SI*4)
    ADDQ $16, SI
    ADDQ $8, DI
    CMPQ SI, CX
    JNE loopAVX2
    VZEROUPPER
    JMP test
loop: // dst[i] = (src[i/2] >> (16 * (i%2))) & 0xFFFF
    MOVQ SI, DI
    MOVQ SI, CX
    SHRQ $1, DI
    ANDQ $0b1111, CX
    SHLQ $4, CX
    MOVL (BX)(DI*4), DI
    SHRL CX, DI
    ANDL $0xFFFF, DI
    MOVL DI, (AX)(SI*4)
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    RET

GLOBL shuffleMask16bits<>(SB), RODATA|NOPTR, $32
DATA shuffleMask16bits<>+0(SB)/4, $0x80800100
DATA shuffleMask16bits<>+4(SB)/4, $0x80800302
DATA shuffleMask16bits<>+8(SB)/4, $0x80800504
DATA shuffleMask16bits<>+12(SB)/4, $0x80800706
DATA shuffleMask16bits<>+16(SB)/4, $0x80800100
DATA shuffleMask16bits<>+20(SB)/4, $0x80800302
DATA shuffleMask16bits<>+24(SB)/4, $0x80800504
DATA shuffleMask16bits<>+28(SB)/4, $0x80800706

// -----------------------------------------------------------------------------
// 64 bits
// -----------------------------------------------------------------------------

// func decodeBlockInt64Default(dst []int64, minDelta, lastValue int64) int64
TEXT ·decodeBlockInt64Default(SB), NOSPLIT, $0-48
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), BX
    MOVQ minDelta+24(FP), CX
    MOVQ lastValue+32(FP), DX
    XORQ SI, SI
    JMP test
loop:
    MOVQ (AX)(SI*8), DI
    ADDQ CX, DI
    ADDQ DI, DX
    MOVQ DX, (AX)(SI*8)
    INCQ SI
test:
    CMPQ SI, BX
    JNE loop
done:
    MOVQ DX, ret+40(FP)
    RET

// func decodeMiniBlockInt64Default(dst []int64, src []uint32, bitWidth uint)
TEXT ·decodeMiniBlockInt64Default(SB), NOSPLIT, $0-56
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), DX
    MOVQ src_base+24(FP), BX
    MOVQ bitWidth+48(FP), CX

    MOVQ $1, R8 // bitMask = (1 << bitWidth) - 1
    SHLQ CX, R8, R8
    DECQ R8
    MOVQ CX, R9 // bitWidth

    XORQ DI, DI // bitOffset
    XORQ SI, SI // index
    XORQ R10, R10
    XORQ R11, R11
    XORQ R14, R14
    JMP test
loop:
    MOVQ DI, R10
    MOVQ DI, CX
    SHRQ $5, R10      // i = bitOffset / 32
    ANDQ $0b11111, CX // j = bitOffset % 32

    MOVLQZX (BX)(R10*4), R11
    MOVQ R8, R12  // d = bitMask
    SHLQ CX, R12  // d = d << j
    ANDQ R12, R11 // d = src[i] & d
    SHRQ CX, R11  // d = d >> j

    MOVQ CX, R13
    ADDQ R9, R13
    CMPQ R13, $32
    JBE next // j+bitWidth <= 32 ?
    MOVQ CX, R15 // j

    MOVLQZX 4(BX)(R10*4), R14
    MOVQ $32, CX
    SUBQ R15, CX  // k = 32 - j
    MOVQ R8, R12  // c = bitMask
    SHRQ CX, R12  // c = c >> k
    ANDQ R12, R14 // c = src[i+1] & c
    SHLQ CX, R14  // c = c << k
    ORQ R14, R11  // d = d | c

    CMPQ R13, $64
    JBE next

    MOVLQZX 8(BX)(R10*4), R14
    MOVQ $64, CX
    SUBQ R15, CX  // k = 64 - j
    MOVQ R8, R12  // c = bitMask
    SHRQ CX, R12  // c = c >> k
    ANDQ R12, R14 // c = src[i+2] & c
    SHLQ CX, R14  // c = c << k
    ORQ R14, R11  // d = d | c
next:
    MOVQ R11, (AX)(SI*8) // dst[n] = d
    ADDQ R9, DI          // bitOffset += bitWidth
    INCQ SI
test:
    CMPQ SI, DX
    JNE loop
    RET
