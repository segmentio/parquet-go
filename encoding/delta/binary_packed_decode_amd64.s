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

// -----------------------------------------------------------------------------
// The decodeMiniBlock* functions below are adaptations of the algorithms
// described in "Decoding billions of integers per second through vectorization"
// from D. Lemire & L. Boytsov, the following changes were made:
//
// - The paper described two methods for decoding integers called "horizontal"
//   and "vertical". The "horizontal" version is the one that applies the best
//   to the bit packing done in the Parquet delta encoding; however, it also
//   differs in some ways, many compression techniques discussed in the paper
//   are not implemented in the Parquet format.
//
// - The paper focuses on implementations based on SSE instructions, which
//   describes how to use PMULLD to emulate the lack of variable bit shift
//   for packed integers. Our version of the bit unpacking algorithms here
//   uses AVX2 and can perform variable bit shifts using VPSRLVD, which yields
//   better throughput since the instruction latency is a single CPU cycles,
//   vs 10 for VPMULLD.
//
// - The reference implementation at https://github.com/lemire/FastPFor/ uses
//   specializations for each bit size, resulting in 32 unique functions.
//   Our version here are more generic, we provide 3 declinaisons of the
//   algorithms for bit widths 1 to 16, 17 to 26, and 27 to 31 (unpacking 32
//   bits values is a simple copy). In that regard, our implementation is
//   somewhat an improvement over the reference, it uses less code and less
//   memory to hold the shuffle masks and shift tables.
//
// Technically, each specialization of our functions could be expressed by the
// algorithm used for unpacking values of 27 to 31 bits. However, multiple steps
// of the main loop can be removed for lower bit widths, providing up to ~35%
// better throughput for smaller sizes. Since we expect delta encoding to often
// result in bit packing values to smaller bit widths, the specializations are
// worth the extra complexity.
//
// For more details, see: https://arxiv.org/pdf/1209.2137v5.pdf
// -----------------------------------------------------------------------------

// decodeMiniBlockInt32x1to16bitsAVX2 is the implementation of the bit unpacking
// algorithm for inputs of bit width 1 to 16.
//
// In this version of the algorithm, we can perform a single memory load in each
// loop iteration since we know that 8 values will fit in a single XMM register.
//
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

    MOVQ CX, R10
    DECQ R10
    SHLQ $5, R10
    ANDQ $0xFF, R10 // (32 * (bitWidth - 1)) % 256

    LEAQ ·shuffleInt32x1to16bits(SB), R11
    VMOVDQA (R11)(R9*1), X1
    VMOVDQA 16(R11)(R9*1), X2

    LEAQ ·shiftRightInt32(SB), R12
    VMOVDQA (R12)(R10*1), X3
    VMOVDQA 16(R12)(R10*1), X4
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
    VZEROUPPER

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

// decodeMiniBlockInt32x17to26bitsAVX2 is the implementation of the bit unpacking
// algorithm for inputs of bit width 17 to 26.
//
// In this version of the algorithm, we need to 32 bytes at each loop iteration
// because 8 bit-packed values will span across two XMM registers.
//
// func decodeMiniBlockInt32x17to26bitsAVX2(dst []int32, src []uint32, bitWidth uint)
TEXT ·decodeMiniBlockInt32x17to26bitsAVX2(SB), NOSPLIT, $56-56
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
    IMULQ $48, R9 // 48 * (bitWidth - 17)

    MOVQ CX, R10
    DECQ R10
    SHLQ $5, R10
    ANDQ $0xFF, R10 // (32 * (bitWidth - 1)) % 256

    LEAQ ·shuffleInt32x17to26bits(SB), R11
    VMOVDQA (R11)(R9*1), X1
    VMOVDQA 16(R11)(R9*1), X2
    VMOVDQA 32(R11)(R9*1), X3

    LEAQ ·shiftRightInt32(SB), R12
    VMOVDQA (R12)(R10*1), X4
    VMOVDQA 16(R12)(R10*1), X5
loop:
    VMOVDQU (BX), X6
    VMOVDQU 16(BX), X7

    VPSHUFB X1, X6, X8
    VPSHUFB X2, X6, X9
    VPSHUFB X3, X7, X10
    VPOR X10, X9, X9

    VPSRLVD X4, X8, X8
    VPSRLVD X5, X9, X9

    VPANDD X0, X8, X8
    VPANDD X0, X9, X9

    VMOVDQU X8, (AX)(SI*4)
    VMOVDQU X9, 16(AX)(SI*4)

    ADDQ CX, BX
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loop
    VZEROUPPER

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

// decodeMiniBlockInt32x27to31bitsAVX2 is the implementation of the bit unpacking
// algorithm for inputs of bit width 27 to 31.
//
// In this version of the algorithm the bit-packed values may span across up to
// 5 bytes. The simpler approach for smaller bit widths where we could perform a
// single shuffle + shift to unpack the values do not work anymore.
//
// Values are unpacked in two steps: the first one extracts lower bits which are
// shifted RIGHT to align on the beginning of 32 bit words, the second extracts
// upper bits which are shifted LEFT to be moved to the end of the 32 bit words.
//
// The amount of LEFT shifts is always "8 minus the amount of RIGHT shift".
//
// func decodeMiniBlockInt32x27to31bitsAVX2(dst []int32, src []uint32, bitWidth uint)
TEXT ·decodeMiniBlockInt32x27to31bitsAVX2(SB), NOSPLIT, $56-56
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
    SUBQ $27, R9
    IMULQ $80, R9 // (80 * (bitWidth - 27))

    MOVQ CX, R10
    DECQ R10
    SHLQ $5, R10
    ANDQ $0xFF, R10 // (32 * (bitWidth - 1)) % 256

    LEAQ ·shuffleInt32x27to31bits(SB), R11
    VMOVDQA (R11)(R9*1), X1
    VMOVDQA 16(R11)(R9*1), X2
    VMOVDQA 32(R11)(R9*1), X3
    VMOVDQA 48(R11)(R9*1), X4
    VMOVDQA 64(R11)(R9*1), X5

    LEAQ ·shiftRightInt32(SB), R12
    LEAQ ·shiftLeftInt32(SB), R13
    VMOVDQA (R12)(R10*1), X6
    VMOVDQA (R13)(R10*1), X7
    VMOVDQA 16(R12)(R10*1), X8
    VMOVDQA 16(R13)(R10*1), X9
loop:
    VMOVDQU (BX), X10
    VMOVDQU 16(BX), X11

    VPSHUFB X1, X10, X12
    VPSHUFB X2, X10, X13
    VPSHUFB X3, X10, X14
    VPSHUFB X4, X11, X15
    VPSHUFB X5, X11, X11

    VPSRLVD X6, X12, X12
    VPSLLVD X7, X13, X13
    VPSRLVD X8, X14, X14
    VPSRLVD X8, X15, X15
    VPSLLVD X9, X11, X11

    VPOR X13, X12, X12
    VPOR X15, X14, X14
    VPOR X11, X14, X14

    VPANDD X0, X12, X12
    VPANDD X0, X14, X14

    VMOVDQU X12, (AX)(SI*4)
    VMOVDQU X14, 16(AX)(SI*4)

    ADDQ CX, BX
    ADDQ $8, SI
    CMPQ SI, DI
    JNE loop
    VZEROUPPER

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
