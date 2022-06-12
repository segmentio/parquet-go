//go:build !purego

#include "funcdata.h"
#include "textflag.h"

#define ok 0
#define errInvalidNegativeValueLength 1

// func decodeLengthValuesDefault(lengths []int32) (sum int, err errno)
TEXT ·decodeLengthValuesDefault(SB), NOSPLIT, $0-40
    MOVQ lengths_base+0(FP), AX
    MOVQ lengths_len+8(FP), CX
    XORQ BX, BX // sum
    XORQ DX, DX // err
    XORQ SI, SI
    XORQ DI, DI
    XORQ R8, R8
    JMP test
loop:
    MOVLQSX (AX)(SI*4), DI
    ADDQ DI, BX
    ORQ DI, R8
    INCQ SI
test:
    CMPQ SI, CX
    JNE loop
    CMPQ R8, $0
    JL invalidNegativeValueLength
done:
    MOVQ BX, sum+24(FP)
    MOVQ DX, err+32(FP)
    RET
invalidNegativeValueLength:
    MOVQ $errInvalidNegativeValueLength, DX
    JMP done

// func decodeLengthValuesAVX2(lengths []int32) (sum int, err errno)
TEXT ·decodeLengthValuesAVX2(SB), NOSPLIT, $0-40
    MOVQ lengths_base+0(FP), AX
    MOVQ lengths_len+8(FP), CX

    XORQ BX, BX // sum
    XORQ DX, DX // err
    XORQ SI, SI
    XORQ DI, DI
    XORQ R8, R8

    CMPQ CX, $16
    JB test

    MOVQ CX, DI
    SHRQ $4, DI
    SHLQ $4, DI

    VPXOR X0, X0, X0 // sums
    VPXOR X1, X1, X1 // bits
loopAVX2:
    VMOVDQU (AX)(SI*4), Y2
    VMOVDQU 32(AX)(SI*4), Y3
    VPADDD Y2, Y0, Y0
    VPADDD Y3, Y0, Y0
    VPOR Y2, Y1, Y1
    VPOR Y3, Y1, Y1
    ADDQ $16, SI
    CMPQ SI, DI
    JNE loopAVX2

    VMOVMSKPS Y1, R8
    CMPQ R8, $0
    JNE invalidNegativeValueLength

    VPSRLDQ $4, Y0, Y1
    VPSRLDQ $8, Y0, Y2
    VPSRLDQ $12, Y0, Y3

    VPADDD Y1, Y0, Y0
    VPADDD Y3, Y2, Y2
    VPADDD Y2, Y0, Y0

    VPERM2I128 $1, Y0, Y0, Y1
    VPADDD Y1, Y0, Y0
    VZEROUPPER
    MOVQ X0, BX
    ANDQ $0x7FFFFFFF, BX

    JMP test
loop:
    MOVL (AX)(SI*4), DI
    ADDL DI, BX
    ORL DI, R8
    INCQ SI
test:
    CMPQ SI, CX
    JNE loop
    CMPL R8, $0
    JL invalidNegativeValueLength
done:
    MOVQ BX, sum+24(FP)
    MOVQ DX, err+32(FP)
    RET
invalidNegativeValueLength:
    MOVQ $errInvalidNegativeValueLength, DX
    JMP done

// func decodeLengthByteArray(dst, src []byte, lengths []int32)
TEXT ·__decodeLengthByteArray(SB), NOSPLIT, $40-72
    MOVQ dst_base+0(FP), AX
    MOVQ src_base+24(FP), BX
    MOVQ lengths_base+48(FP), DX
    MOVQ lengths_len+56(FP), DI
    RET
