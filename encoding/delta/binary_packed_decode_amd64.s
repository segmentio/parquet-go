//go:build !purego

#include "funcdata.h"
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

// func decodeMiniBlockInt32x1to16bitsAVX2(dst []int32, src []uint32, bitWidth uint)
TEXT ·decodeMiniBlockInt32x1to16bitsAVX2(SB), NOSPLIT, $56-56
    NO_LOCAL_POINTERS
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), DX
    MOVQ src_base+24(FP), BX
    MOVQ bitWidth+48(FP), CX

    CMPQ DX, $8
    JB tail

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI
    XORQ SI, SI

    MOVQ $1, R8
    SHLQ CX, R8
    DECQ R8
    MOVQ R8, X0
    VPBROADCASTD X0, X0

    MOVQ CX, R9
    DECQ R9
    SHLQ $5, R9 // 32 * (bitWidth - 1)

    LEAQ shuffleInt32x1to16bits<>(SB), R10
    LEAQ shiftInt32x1to16bits<>(SB), R11
    VMOVDQA (R10)(R9*1), X1
    VMOVDQA 16(R10)(R9*1), X2
    VMOVDQA (R11)(R9*1), X3
    VMOVDQA 16(R11)(R9*1), X4
loop:
    VMOVDQU (BX), X7

    VPSHUFB X1, X7, X5
    VPSHUFB X2, X7, X6

    VPSRLVD X3, X5, X5
    VPSRLVD X4, X6, X6

    VPANDD X0, X5, X5
    VPANDD X0, X6, X6

    VMOVDQU X5, (AX)(SI*4)
    VMOVDQU X6, 16(AX)(SI*4)

    ADDQ CX, BX
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loop

    CMPQ SI, DX
    JE done
    LEAQ (AX)(SI*4), AX
    SUBQ SI, DX
tail:
    MOVQ AX, dst_base-56(SP)
    MOVQ DX, dst_len-48(SP)
    MOVQ BX, src_base-32(SP)
    MOVQ CX, bitWidth-8(SP)
    CALL ·decodeMiniBlockInt32Default(SB)
done:
    RET

// Shuffle masks used to broadcast bytes of bit-packed valued into vector
// registers at positions where they can then be shifted into the right
// locations.
//
// The bitWidth is used to offset where in the arrays the masks are located
// with the following operations:
//
//      mask = array + (32 * (bitWidth - 1))
//
// The constant 32 here represents the fact that we have to work on groups
// of 8 values for the bit-packed numbers to be aligned on byte boundaries.
// 8 values fit in 2 XMM registers, 16 bytes each, 32 bytes total.
GLOBL shuffleInt32x1to16bits<>(SB), RODATA|NOPTR, $512
GLOBL shiftInt32x1to16bits<>(SB),   RODATA|NOPTR, $512

#define offset1to16(bitWidth) (32 * (bitWidth - 1))

// 1 bit => 32 bits
// -----------------
// 0: [a,b,c,d,e,f,g,h]
// ...
DATA shuffleInt32x1to16bits<>+0+0(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+0+4(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+0+8(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+0+12(SB)/4, $0x80808000
DATA shuffleInt32x1to16bits<>+0+16(SB)/4, $0x80808000
DATA shuffleInt32x1to16bits<>+0+20(SB)/4, $0x80808000
DATA shuffleInt32x1to16bits<>+0+24(SB)/4, $0x80808000
DATA shuffleInt32x1to16bits<>+0+28(SB)/4, $0x80808000

DATA shiftInt32x1to16bits<>+0+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+0+4(SB)/4,  $1
DATA shiftInt32x1to16bits<>+0+8(SB)/4,  $2
DATA shiftInt32x1to16bits<>+0+12(SB)/4, $3
DATA shiftInt32x1to16bits<>+0+16(SB)/4, $4
DATA shiftInt32x1to16bits<>+0+20(SB)/4, $5
DATA shiftInt32x1to16bits<>+0+24(SB)/4, $6
DATA shiftInt32x1to16bits<>+0+28(SB)/4, $7

// 2 bits => 32 bits
// -----------------
// 0: [a,a,b,b,c,c,d,d]
// 1: [e,e,f,f,g,g,h,h]
// ...
DATA shuffleInt32x1to16bits<>+32+0(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+32+4(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+32+8(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+32+12(SB)/4, $0x80808000
DATA shuffleInt32x1to16bits<>+32+16(SB)/4, $0x80808001
DATA shuffleInt32x1to16bits<>+32+20(SB)/4, $0x80808001
DATA shuffleInt32x1to16bits<>+32+24(SB)/4, $0x80808001
DATA shuffleInt32x1to16bits<>+32+28(SB)/4, $0x80808001

DATA shiftInt32x1to16bits<>+32+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+32+4(SB)/4,  $2
DATA shiftInt32x1to16bits<>+32+8(SB)/4,  $4
DATA shiftInt32x1to16bits<>+32+12(SB)/4, $6
DATA shiftInt32x1to16bits<>+32+16(SB)/4, $0
DATA shiftInt32x1to16bits<>+32+20(SB)/4, $2
DATA shiftInt32x1to16bits<>+32+24(SB)/4, $4
DATA shiftInt32x1to16bits<>+32+28(SB)/4, $6

// 3 bits => 32 bits
// -----------------
// 0: [a,a,a,b,b,b,c,c]
// 1: [c,d,d,d,e,e,e,f]
// 2: [f,f,g,g,g,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+64+0(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+64+4(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+64+8(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+64+12(SB)/4, $0x80808001
DATA shuffleInt32x1to16bits<>+64+16(SB)/4, $0x80808001
DATA shuffleInt32x1to16bits<>+64+20(SB)/4, $0x80800201
DATA shuffleInt32x1to16bits<>+64+24(SB)/4, $0x80808002
DATA shuffleInt32x1to16bits<>+64+28(SB)/4, $0x80808002

DATA shiftInt32x1to16bits<>+64+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+64+4(SB)/4,  $3
DATA shiftInt32x1to16bits<>+64+8(SB)/4,  $6
DATA shiftInt32x1to16bits<>+64+12(SB)/4, $1
DATA shiftInt32x1to16bits<>+64+16(SB)/4, $4
DATA shiftInt32x1to16bits<>+64+20(SB)/4, $7
DATA shiftInt32x1to16bits<>+64+24(SB)/4, $2
DATA shiftInt32x1to16bits<>+64+28(SB)/4, $5

// 4 bits => 32 bits
// -----------------
// 0: [a,a,a,a,b,b,b,b]
// 1: [c,c,c,c,d,d,d,d]
// 2: [e,e,e,e,f,f,f,f]
// 3: [g,g,g,g,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+96+0(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+96+4(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+96+8(SB)/4,  $0x80808001
DATA shuffleInt32x1to16bits<>+96+12(SB)/4, $0x80808001
DATA shuffleInt32x1to16bits<>+96+16(SB)/4, $0x80808002
DATA shuffleInt32x1to16bits<>+96+20(SB)/4, $0x80808002
DATA shuffleInt32x1to16bits<>+96+24(SB)/4, $0x80808003
DATA shuffleInt32x1to16bits<>+96+28(SB)/4, $0x80808003

DATA shiftInt32x1to16bits<>+96+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+96+4(SB)/4,  $4
DATA shiftInt32x1to16bits<>+96+8(SB)/4,  $0
DATA shiftInt32x1to16bits<>+96+12(SB)/4, $4
DATA shiftInt32x1to16bits<>+96+16(SB)/4, $0
DATA shiftInt32x1to16bits<>+96+20(SB)/4, $4
DATA shiftInt32x1to16bits<>+96+24(SB)/4, $0
DATA shiftInt32x1to16bits<>+96+28(SB)/4, $4

// 5 bits => 32 bits
// -----------------
// 0: [a,a,a,a,a,b,b,b]
// 1: [b,b,c,c,c,c,c,d]
// 2: [d,d,d,d,e,e,e,e]
// 3: [e,f,f,f,f,f,g,g]
// 4: [g,g,g,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+128+0(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+128+4(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+128+8(SB)/4,  $0x80808001
DATA shuffleInt32x1to16bits<>+128+12(SB)/4, $0x80800201
DATA shuffleInt32x1to16bits<>+128+16(SB)/4, $0x80800302
DATA shuffleInt32x1to16bits<>+128+20(SB)/4, $0x80808003
DATA shuffleInt32x1to16bits<>+128+24(SB)/4, $0x80800403
DATA shuffleInt32x1to16bits<>+128+28(SB)/4, $0x80808004

DATA shiftInt32x1to16bits<>+128+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+128+4(SB)/4,  $5
DATA shiftInt32x1to16bits<>+128+8(SB)/4,  $2
DATA shiftInt32x1to16bits<>+128+12(SB)/4, $7
DATA shiftInt32x1to16bits<>+128+16(SB)/4, $4
DATA shiftInt32x1to16bits<>+128+20(SB)/4, $1
DATA shiftInt32x1to16bits<>+128+24(SB)/4, $6
DATA shiftInt32x1to16bits<>+128+28(SB)/4, $3

// 6 bits => 32 bits
// -----------------
// 0: [a,a,a,a,a,a,b,b]
// 1: [b,b,b,b,c,c,c,c]
// 2: [c,c,d,d,d,d,d,d]
// 3: [e,e,e,e,e,e,f,f]
// 4: [f,f,f,f,g,g,g,g]
// 5: [g,g,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+160+0(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+160+4(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+160+8(SB)/4,  $0x80800201
DATA shuffleInt32x1to16bits<>+160+12(SB)/4, $0x80808002
DATA shuffleInt32x1to16bits<>+160+16(SB)/4, $0x80808003
DATA shuffleInt32x1to16bits<>+160+20(SB)/4, $0x80800403
DATA shuffleInt32x1to16bits<>+160+24(SB)/4, $0x80800504
DATA shuffleInt32x1to16bits<>+160+28(SB)/4, $0x80808005

DATA shiftInt32x1to16bits<>+160+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+160+4(SB)/4,  $6
DATA shiftInt32x1to16bits<>+160+8(SB)/4,  $4
DATA shiftInt32x1to16bits<>+160+12(SB)/4, $2
DATA shiftInt32x1to16bits<>+160+16(SB)/4, $0
DATA shiftInt32x1to16bits<>+160+20(SB)/4, $6
DATA shiftInt32x1to16bits<>+160+24(SB)/4, $4
DATA shiftInt32x1to16bits<>+160+28(SB)/4, $2

// 7 bits => 32 bits
// -----------------
// 0: [a,a,a,a,a,a,a,b]
// 1: [b,b,b,b,b,b,c,c]
// 2: [c,c,c,c,c,d,d,d]
// 3: [d,d,d,d,e,e,e,e]
// 4: [e,e,e,f,f,f,f,f]
// 5: [f,f,g,g,g,g,g,g]
// 6: [g,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+192+0(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+192+4(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+192+8(SB)/4,  $0x80800201
DATA shuffleInt32x1to16bits<>+192+12(SB)/4, $0x80800302
DATA shuffleInt32x1to16bits<>+192+16(SB)/4, $0x80800403
DATA shuffleInt32x1to16bits<>+192+20(SB)/4, $0x80800504
DATA shuffleInt32x1to16bits<>+192+24(SB)/4, $0x80800605
DATA shuffleInt32x1to16bits<>+192+28(SB)/4, $0x80808006

DATA shiftInt32x1to16bits<>+192+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+192+4(SB)/4,  $7
DATA shiftInt32x1to16bits<>+192+8(SB)/4,  $6
DATA shiftInt32x1to16bits<>+192+12(SB)/4, $5
DATA shiftInt32x1to16bits<>+192+16(SB)/4, $4
DATA shiftInt32x1to16bits<>+192+20(SB)/4, $3
DATA shiftInt32x1to16bits<>+192+24(SB)/4, $2
DATA shiftInt32x1to16bits<>+192+28(SB)/4, $1

// 8 bits => 32 bits
// -----------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [b,b,b,b,b,b,b,b]
// 2: [c,c,c,c,c,c,c,c]
// 3: [d,d,d,d,d,d,d,d]
// 4: [e,e,e,e,e,e,e,e]
// 5: [f,f,f,f,f,f,f,f]
// 6: [g,g,g,g,g,g,g,g]
// 7: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+224+0(SB)/4,  $0x80808000
DATA shuffleInt32x1to16bits<>+224+4(SB)/4,  $0x80808001
DATA shuffleInt32x1to16bits<>+224+8(SB)/4,  $0x80808002
DATA shuffleInt32x1to16bits<>+224+12(SB)/4, $0x80808003
DATA shuffleInt32x1to16bits<>+224+16(SB)/4, $0x80808004
DATA shuffleInt32x1to16bits<>+224+20(SB)/4, $0x80808005
DATA shuffleInt32x1to16bits<>+224+24(SB)/4, $0x80808006
DATA shuffleInt32x1to16bits<>+224+28(SB)/4, $0x80808007

DATA shiftInt32x1to16bits<>+224+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+224+4(SB)/4,  $0
DATA shiftInt32x1to16bits<>+224+8(SB)/4,  $0
DATA shiftInt32x1to16bits<>+224+12(SB)/4, $0
DATA shiftInt32x1to16bits<>+224+16(SB)/4, $0
DATA shiftInt32x1to16bits<>+224+20(SB)/4, $0
DATA shiftInt32x1to16bits<>+224+24(SB)/4, $0
DATA shiftInt32x1to16bits<>+224+28(SB)/4, $0

// 9 bits => 32 bits
// -----------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,b,b,b,b,b,b,b]
// 2: [b,b,c,c,c,c,c,c]
// 3: [c,c,c,d,d,d,d,d]
// 4: [d,d,d,d,e,e,e,e]
// 5: [e,e,e,e,e,f,f,f]
// 6: [f,f,f,f,f,f,g,g]
// 7: [g,g,g,g,g,g,g,h]
// 8: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+256+0(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+256+4(SB)/4,  $0x80800201
DATA shuffleInt32x1to16bits<>+256+8(SB)/4,  $0x80800302
DATA shuffleInt32x1to16bits<>+256+12(SB)/4, $0x80800403
DATA shuffleInt32x1to16bits<>+256+16(SB)/4, $0x80800504
DATA shuffleInt32x1to16bits<>+256+20(SB)/4, $0x80800605
DATA shuffleInt32x1to16bits<>+256+24(SB)/4, $0x80800706
DATA shuffleInt32x1to16bits<>+256+28(SB)/4, $0x80800807

DATA shiftInt32x1to16bits<>+256+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+256+4(SB)/4,  $1
DATA shiftInt32x1to16bits<>+256+8(SB)/4,  $2
DATA shiftInt32x1to16bits<>+256+12(SB)/4, $3
DATA shiftInt32x1to16bits<>+256+16(SB)/4, $4
DATA shiftInt32x1to16bits<>+256+20(SB)/4, $5
DATA shiftInt32x1to16bits<>+256+24(SB)/4, $6
DATA shiftInt32x1to16bits<>+256+28(SB)/4, $7

// 10 bits => 32 bits
// ------------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,a,b,b,b,b,b,b]
// 2: [b,b,b,b,c,c,c,c]
// 3: [c,c,c,c,c,c,d,d]
// 4: [d,d,d,d,d,d,d,d]
// 5: [e,e,e,e,e,e,e,e]
// 6: [e,e,f,f,f,f,f,f]
// 7: [f,f,f,f,g,g,g,g]
// 8: [g,g,g,g,g,g,h,h]
// 9: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+288+0(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+288+4(SB)/4,  $0x80800201
DATA shuffleInt32x1to16bits<>+288+8(SB)/4,  $0x80800302
DATA shuffleInt32x1to16bits<>+288+12(SB)/4, $0x80800403
DATA shuffleInt32x1to16bits<>+288+16(SB)/4, $0x80800605
DATA shuffleInt32x1to16bits<>+288+20(SB)/4, $0x80800706
DATA shuffleInt32x1to16bits<>+288+24(SB)/4, $0x80800807
DATA shuffleInt32x1to16bits<>+288+28(SB)/4, $0x80800908

DATA shiftInt32x1to16bits<>+288+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+288+4(SB)/4,  $2
DATA shiftInt32x1to16bits<>+288+8(SB)/4,  $4
DATA shiftInt32x1to16bits<>+288+12(SB)/4, $6
DATA shiftInt32x1to16bits<>+288+16(SB)/4, $0
DATA shiftInt32x1to16bits<>+288+20(SB)/4, $2
DATA shiftInt32x1to16bits<>+288+24(SB)/4, $4
DATA shiftInt32x1to16bits<>+288+28(SB)/4, $6

// 11 bits => 32 bits
// ------------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,a,a,b,b,b,b,b]
// 2: [b,b,b,b,b,b,c,c]
// 3: [c,c,c,c,c,c,c,c]
// 4: [c,d,d,d,d,d,d,d]
// 5: [d,d,d,d,e,e,e,e]
// 6: [e,e,e,e,e,e,e,f]
// 7: [f,f,f,f,f,f,f,f]
// 8: [f,f,g,g,g,g,g,g]
// 9: [g,g,g,g,g,h,h,h]
// A: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+320+0(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+320+4(SB)/4,  $0x80800201
DATA shuffleInt32x1to16bits<>+320+8(SB)/4,  $0x80040302
DATA shuffleInt32x1to16bits<>+320+12(SB)/4, $0x80800504
DATA shuffleInt32x1to16bits<>+320+16(SB)/4, $0x80800605
DATA shuffleInt32x1to16bits<>+320+20(SB)/4, $0x80080706
DATA shuffleInt32x1to16bits<>+320+24(SB)/4, $0x80800908
DATA shuffleInt32x1to16bits<>+320+28(SB)/4, $0x80800A09

DATA shiftInt32x1to16bits<>+320+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+320+4(SB)/4,  $3
DATA shiftInt32x1to16bits<>+320+8(SB)/4,  $6
DATA shiftInt32x1to16bits<>+320+12(SB)/4, $1
DATA shiftInt32x1to16bits<>+320+16(SB)/4, $4
DATA shiftInt32x1to16bits<>+320+20(SB)/4, $7
DATA shiftInt32x1to16bits<>+320+24(SB)/4, $2
DATA shiftInt32x1to16bits<>+320+28(SB)/4, $5

// 12 bits => 32 bits
// ------------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,a,a,a,b,b,b,b]
// 2: [b,b,b,b,b,b,b,b]
// 3: [c,c,c,c,c,c,c,c]
// 4: [c,c,c,c,d,d,d,d]
// 5: [d,d,d,d,d,d,d,d]
// 6: [e,e,e,e,e,e,e,e]
// 7: [e,e,e,e,f,f,f,f]
// 8: [f,f,f,f,f,f,f,f]
// 9: [g,g,g,g,g,g,g,g]
// A: [g,g,g,g,h,h,h,h]
// B: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+352+0(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+352+4(SB)/4,  $0x80800201
DATA shuffleInt32x1to16bits<>+352+8(SB)/4,  $0x80080403
DATA shuffleInt32x1to16bits<>+352+12(SB)/4, $0x80800504
DATA shuffleInt32x1to16bits<>+352+16(SB)/4, $0x80800706
DATA shuffleInt32x1to16bits<>+352+20(SB)/4, $0x80800807
DATA shuffleInt32x1to16bits<>+352+24(SB)/4, $0x80800A09
DATA shuffleInt32x1to16bits<>+352+28(SB)/4, $0x80800B0A

DATA shiftInt32x1to16bits<>+352+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+352+4(SB)/4,  $4
DATA shiftInt32x1to16bits<>+352+8(SB)/4,  $0
DATA shiftInt32x1to16bits<>+352+12(SB)/4, $4
DATA shiftInt32x1to16bits<>+352+16(SB)/4, $0
DATA shiftInt32x1to16bits<>+352+20(SB)/4, $4
DATA shiftInt32x1to16bits<>+352+24(SB)/4, $0
DATA shiftInt32x1to16bits<>+352+28(SB)/4, $4

// 13 bits => 32 bits
// ------------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,a,a,a,a,b,b,b]
// 2: [b,b,b,b,b,b,b,b]
// 3: [b,b,c,c,c,c,c,c]
// 4: [c,c,c,c,c,c,c,d]
// 5: [d,d,d,d,d,d,d,d]
// 6: [d,d,d,d,e,e,e,e]
// 7: [e,e,e,e,e,e,e,e]
// 8: [e,f,f,f,f,f,f,f]
// 9: [f,f,f,f,f,f,g,g]
// A: [g,g,g,g,g,g,g,g]
// B: [g,g,g,h,h,h,h,h]
// C: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+384+0(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+384+4(SB)/4,  $0x80030201
DATA shuffleInt32x1to16bits<>+384+8(SB)/4,  $0x80800403
DATA shuffleInt32x1to16bits<>+384+12(SB)/4, $0x80060504
DATA shuffleInt32x1to16bits<>+384+16(SB)/4, $0x80080706
DATA shuffleInt32x1to16bits<>+384+20(SB)/4, $0x80800908
DATA shuffleInt32x1to16bits<>+384+24(SB)/4, $0x800B0A09
DATA shuffleInt32x1to16bits<>+384+28(SB)/4, $0x80800C0B

DATA shiftInt32x1to16bits<>+384+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+384+4(SB)/4,  $5
DATA shiftInt32x1to16bits<>+384+8(SB)/4,  $2
DATA shiftInt32x1to16bits<>+384+12(SB)/4, $7
DATA shiftInt32x1to16bits<>+384+16(SB)/4, $4
DATA shiftInt32x1to16bits<>+384+20(SB)/4, $1
DATA shiftInt32x1to16bits<>+384+24(SB)/4, $6
DATA shiftInt32x1to16bits<>+384+28(SB)/4, $3

// 14 bits => 32 bits
// ------------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,a,a,a,a,a,b,b]
// 2: [b,b,b,b,b,b,b,b]
// 3: [b,b,b,b,c,c,c,c]
// 4: [c,c,c,c,c,c,c,c]
// 5: [c,c,d,d,d,d,d,d]
// 6: [d,d,d,d,d,d,d,d]
// 7: [e,e,e,e,e,e,e,e]
// 8: [e,e,e,e,e,e,f,f]
// 9: [f,f,f,f,f,f,f,f]
// A: [f,f,f,f,g,g,g,g]
// B: [g,g,g,g,g,g,g,g]
// C: [g,g,h,h,h,h,h,h]
// D: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+416+0(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+416+4(SB)/4,  $0x80030201
DATA shuffleInt32x1to16bits<>+416+8(SB)/4,  $0x80050403
DATA shuffleInt32x1to16bits<>+416+12(SB)/4, $0x80800605
DATA shuffleInt32x1to16bits<>+416+16(SB)/4, $0x80080807
DATA shuffleInt32x1to16bits<>+416+20(SB)/4, $0x800A0908
DATA shuffleInt32x1to16bits<>+416+24(SB)/4, $0x800C0B0A
DATA shuffleInt32x1to16bits<>+416+28(SB)/4, $0x80800D0C

DATA shiftInt32x1to16bits<>+416+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+416+4(SB)/4,  $6
DATA shiftInt32x1to16bits<>+416+8(SB)/4,  $4
DATA shiftInt32x1to16bits<>+416+12(SB)/4, $2
DATA shiftInt32x1to16bits<>+416+16(SB)/4, $0
DATA shiftInt32x1to16bits<>+416+20(SB)/4, $6
DATA shiftInt32x1to16bits<>+416+24(SB)/4, $4
DATA shiftInt32x1to16bits<>+416+28(SB)/4, $2

// 15 bits => 32 bits
// ------------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,a,a,a,a,a,a,b]
// 2: [b,b,b,b,b,b,b,b]
// 3: [b,b,b,b,b,b,c,c]
// 4: [c,c,c,c,c,c,c,c]
// 5: [c,c,c,c,c,d,d,d]
// 6: [d,d,d,d,d,d,d,d]
// 7: [d,d,d,d,e,e,e,e]
// 8: [e,e,e,e,e,e,e,e]
// 9: [e,e,e,f,f,f,f,f]
// A: [f,f,f,f,f,f,f,f]
// B: [f,f,g,g,g,g,g,g]
// C: [g,g,g,g,g,g,g,g]
// D: [g,h,h,h,h,h,h,h]
// E: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+448+0(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+448+4(SB)/4,  $0x80030201
DATA shuffleInt32x1to16bits<>+448+8(SB)/4,  $0x80050403
DATA shuffleInt32x1to16bits<>+448+12(SB)/4, $0x80070605
DATA shuffleInt32x1to16bits<>+448+16(SB)/4, $0x80090807
DATA shuffleInt32x1to16bits<>+448+20(SB)/4, $0x800B0A09
DATA shuffleInt32x1to16bits<>+448+24(SB)/4, $0x800D0C0B
DATA shuffleInt32x1to16bits<>+448+28(SB)/4, $0x80800E0D

DATA shiftInt32x1to16bits<>+448+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+448+4(SB)/4,  $7
DATA shiftInt32x1to16bits<>+448+8(SB)/4,  $6
DATA shiftInt32x1to16bits<>+448+12(SB)/4, $5
DATA shiftInt32x1to16bits<>+448+16(SB)/4, $4
DATA shiftInt32x1to16bits<>+448+20(SB)/4, $3
DATA shiftInt32x1to16bits<>+448+24(SB)/4, $2
DATA shiftInt32x1to16bits<>+448+28(SB)/4, $1

// 16 bits => 32 bits
// ------------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,a,a,a,a,a,a,a]
// 2: [b,b,b,b,b,b,b,b]
// 3: [b,b,b,b,b,b,c,b]
// 4: [c,c,c,c,c,c,c,c]
// 5: [c,c,c,c,c,c,c,c]
// 6: [d,d,d,d,d,d,d,d]
// 7: [d,d,d,d,d,d,d,d]
// 8: [e,e,e,e,e,e,e,e]
// 9: [e,e,e,e,e,e,e,e]
// A: [f,f,f,f,f,f,f,f]
// B: [f,f,f,f,f,f,f,f]
// C: [g,g,g,g,g,g,g,g]
// D: [g,g,g,g,g,g,g,g]
// E: [h,h,h,h,h,h,h,h]
// F: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x1to16bits<>+480+0(SB)/4,  $0x80800100
DATA shuffleInt32x1to16bits<>+480+4(SB)/4,  $0x80800302
DATA shuffleInt32x1to16bits<>+480+8(SB)/4,  $0x80800504
DATA shuffleInt32x1to16bits<>+480+12(SB)/4, $0x80800706
DATA shuffleInt32x1to16bits<>+480+16(SB)/4, $0x80800908
DATA shuffleInt32x1to16bits<>+480+20(SB)/4, $0x80800B0A
DATA shuffleInt32x1to16bits<>+480+24(SB)/4, $0x80800D0C
DATA shuffleInt32x1to16bits<>+480+28(SB)/4, $0x80800F0E

DATA shiftInt32x1to16bits<>+480+0(SB)/4,  $0
DATA shiftInt32x1to16bits<>+480+4(SB)/4,  $0
DATA shiftInt32x1to16bits<>+480+8(SB)/4,  $0
DATA shiftInt32x1to16bits<>+480+12(SB)/4, $0
DATA shiftInt32x1to16bits<>+480+16(SB)/4, $0
DATA shiftInt32x1to16bits<>+480+20(SB)/4, $0
DATA shiftInt32x1to16bits<>+480+24(SB)/4, $0
DATA shiftInt32x1to16bits<>+480+28(SB)/4, $0

// func decodeMiniBlockInt32x17to31bitsAVX2(dst []int32, src []uint32, bitWidth uint)
TEXT ·decodeMiniBlockInt32x17to31bitsAVX2(SB), NOSPLIT, $56-56
    NO_LOCAL_POINTERS
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), DX
    MOVQ src_base+24(FP), BX
    MOVQ bitWidth+48(FP), CX

    CMPQ DX, $8
    JB tail

    MOVQ DX, DI
    SHRQ $3, DI
    SHLQ $3, DI
    XORQ SI, SI

    MOVQ $1, R8
    SHLQ CX, R8
    DECQ R8
    MOVQ R8, X0
    VPBROADCASTD X0, X0

    MOVQ CX, R9
    SUBQ $17, R9
    IMULQ $48, R9 // 48 * (bitWidth - (16 + 1))

    MOVQ CX, R10
    SUBQ $17, R10
    SHLQ $5, R10  // 32 * (bitWidth - (16 + 1))

    LEAQ shuffleInt32x17to31bits<>(SB), R11
    VMOVDQA (R11)(R9*1), X1
    VMOVDQA 16(R11)(R9*1), X2
    VMOVDQA 32(R11)(R9*1), X3

    LEAQ shiftInt32x17to31bits<>(SB), R12
    VMOVDQA (R12)(R10*1), X4
    VMOVDQA 16(R12)(R10*1), X5
    VMOVDQA 32(R12)(R10*1), X6
loop:
    VMOVDQU (BX), X7
    VMOVDQU 16(BX), X8

    VPSHUFB X1, X7, X9
    VPSHUFB X2, X7, X10
    VPSHUFB X3, X8, X11
    VPOR X11, X10, X10

    VPSRLVD X4, X9, X9
    VPSRLVD X5, X10, X10

    VPANDD X0, X9, X9
    VPANDD X0, X10, X10

    VMOVDQU X9, (AX)(SI*4)
    VMOVDQU X10, 16(AX)(SI*4)

    ADDQ CX, BX
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loop

    CMPQ SI, DX
    JE done
    LEAQ (AX)(SI*4), AX
    SUBQ SI, DX
tail:
    MOVQ AX, dst_base-56(SP)
    MOVQ DX, dst_len-48(SP)
    MOVQ BX, src_base-32(SP)
    MOVQ CX, bitWidth-8(SP)
    CALL ·decodeMiniBlockInt32Default(SB)
done:
    RET

GLOBL shuffleInt32x17to31bits<>(SB), RODATA|NOPTR, $720
GLOBL shiftInt32x17to31bits<>(SB),   RODATA|NOPTR, $480

// 17 bits => 32 bits
// ------------------
// 0: [a,a,a,a,a,a,a,a]
// 1: [a,a,a,a,a,a,a,a]
// 2: [a,b,b,b,b,b,b,b]
// 3: [b,b,b,b,b,b,b,b]
// 4: [b,b,c,c,c,c,c,c]
// 5: [c,c,c,c,c,c,c,c]
// 6: [c,c,c,d,d,d,d,d]
// 7: [d,d,d,d,d,d,d,d]
// 8: [d,d,d,d,e,e,e,e]
// 9: [e,e,e,e,e,e,e,e]
// A: [e,e,e,e,e,f,f,f]
// B: [f,f,f,f,f,f,f,f]
// C: [f,f,f,f,f,f,g,g]
// D: [g,g,g,g,g,g,g,g]
// E: [g,g,g,g,g,g,g,h]
// F: [h,h,h,h,h,h,h,h]
// ---
// 0: [h,h,h,h,h,h,h,h]
// ...
DATA shuffleInt32x17to31bits<>+0+0(SB)/4,  $0x80020100
DATA shuffleInt32x17to31bits<>+0+4(SB)/4,  $0x80040302
DATA shuffleInt32x17to31bits<>+0+8(SB)/4,  $0x80060504
DATA shuffleInt32x17to31bits<>+0+12(SB)/4, $0x80080706

DATA shuffleInt32x17to31bits<>+0+16(SB)/4, $0x800A0908
DATA shuffleInt32x17to31bits<>+0+20(SB)/4, $0x800C0B0A
DATA shuffleInt32x17to31bits<>+0+24(SB)/4, $0x800E0D0C
DATA shuffleInt32x17to31bits<>+0+28(SB)/4, $0x80800F0E

DATA shuffleInt32x17to31bits<>+0+32(SB)/4, $0x80008080
DATA shuffleInt32x17to31bits<>+0+36(SB)/4, $0x80808080
DATA shuffleInt32x17to31bits<>+0+40(SB)/4, $0x80808080
DATA shuffleInt32x17to31bits<>+0+44(SB)/4, $0x80808080

DATA shiftInt32x17to31bits<>+0+0(SB)/4,  $0
DATA shiftInt32x17to31bits<>+0+4(SB)/4,  $1
DATA shiftInt32x17to31bits<>+0+8(SB)/4,  $2
DATA shiftInt32x17to31bits<>+0+12(SB)/4, $3

DATA shiftInt32x17to31bits<>+0+16(SB)/4, $4
DATA shiftInt32x17to31bits<>+0+20(SB)/4, $5
DATA shiftInt32x17to31bits<>+0+24(SB)/4, $6
DATA shiftInt32x17to31bits<>+0+28(SB)/4, $7

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
