//go:build !purego

#include "funcdata.h"
#include "textflag.h"

// func validatePrefixAndSuffixLengthValuesAVX2(prefix, suffix []int32, maxLength int) (totalPrefixLength, totalSuffixLength int, ok bool)
TEXT ·validatePrefixAndSuffixLengthValuesAVX2(SB), NOSPLIT, $0-73
    MOVQ prefix_base+0(FP), AX
    MOVQ suffix_base+24(FP), BX
    MOVQ suffix_len+32(FP), CX
    MOVQ maxLength+48(FP), DX

    XORQ SI, SI
    XORQ DI, DI // lastValueLength
    XORQ R8, R8
    XORQ R9, R9
    XORQ R10, R10 // totalPrefixLength
    XORQ R11, R11 // totalSuffixLength
    XORQ R12, R12 // ok

    CMPQ CX, $8
    JB test

    MOVQ CX, R13
    SHRQ $3, R13
    SHLQ $3, R13

    VPXOR X0, X0, X0 // lastValueLengths
    VPXOR X1, X1, X1 // totalPrefixLengths
    VPXOR X2, X2, X2 // totalSuffixLengths
    VPXOR X3, X3, X3 // negative prefix length sentinels
    VPXOR X4, X4, X4 // negative suffix length sentinels
    VPXOR X5, X5, X5 // prefix length overflow sentinels
    VMOVDQU ·rotateLeft32(SB), Y6

loopAVX2:
    VMOVDQU (AX)(SI*4), Y7 // p
    VMOVDQU (BX)(SI*4), Y8 // n

    VPADDD Y7, Y1, Y1
    VPADDD Y8, Y2, Y2

    VPOR Y7, Y3, Y3
    VPOR Y8, Y4, Y4

    VPADDD Y7, Y8, Y9 // p + n
    VPERMD Y0, Y6, Y10
    VPBLENDD $1, Y10, Y9, Y10
    VPCMPGTD Y10, Y7, Y10
    VPOR Y10, Y5, Y5

    VMOVDQU Y9, Y0
    ADDQ $8, SI
    CMPQ SI, R13
    JNE loopAVX2

    // If any of the sentinel values has its most significant bit set then one
    // of the values was negative or one of the prefixes was greater than the
    // length of the previous value, return false.
    VPOR Y4, Y3, Y3
    VPOR Y5, Y3, Y3
    VMOVMSKPS Y3, R13
    CMPQ R13, $0
    JNE done

    // We computed 8 sums in parallel for the prefix and suffix arrays, they
    // need to be accumulated into single values, which is what these reduction
    // steps do.
    VPSRLDQ $4, Y1, Y5
    VPSRLDQ $8, Y1, Y6
    VPSRLDQ $12, Y1, Y7
    VPADDD Y5, Y1, Y1
    VPADDD Y6, Y1, Y1
    VPADDD Y7, Y1, Y1
    VPERM2I128 $1, Y1, Y1, Y0
    VPADDD Y0, Y1, Y1
    MOVQ X1, R10
    ANDQ $0x7FFFFFFF, R10

    VPSRLDQ $4, Y2, Y5
    VPSRLDQ $8, Y2, Y6
    VPSRLDQ $12, Y2, Y7
    VPADDD Y5, Y2, Y2
    VPADDD Y6, Y2, Y2
    VPADDD Y7, Y2, Y2
    VPERM2I128 $1, Y2, Y2, Y0
    VPADDD Y0, Y2, Y2
    MOVQ X2, R11
    ANDQ $0x7FFFFFFF, R11

    JMP test
loop:
    MOVLQSX (AX)(SI*4), R8
    MOVLQSX (BX)(SI*4), R9

    CMPQ R8, $0 // p < 0 ?
    JL done

    CMPQ R9, $0 // n < 0 ?
    JL done

    CMPQ R8, DI // p > lastValueLength ?
    JG done

    ADDQ R8, R10
    ADDQ R9, R11
    ADDQ R8, DI
    ADDQ R9, DI

    INCQ SI
test:
    CMPQ SI, CX
    JNE loop

    CMPQ R11, DX // totalSuffixLength > maxLength ?
    JG done

    MOVB $1, R12
done:
    MOVQ R10, totalPrefixLength+56(FP)
    MOVQ R11, totalSuffixLength+64(FP)
    MOVB R12, ok+72(FP)
    RET
