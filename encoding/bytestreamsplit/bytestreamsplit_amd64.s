 //go:build !purego

#include "textflag.h"

// func encodeFloat(dst, src []byte)
TEXT ·encodeFloat(SB), NOSPLIT, $0-48
    MOVQ src_base+24(FP), AX
    MOVQ src_len+32(FP), BX
    MOVQ dst_base+0(FP), R8

    MOVQ AX, DX
    ADDQ BX, DX

    SHRQ $2, BX
    CMPQ BX, $0
    JE done

    MOVQ R8, R9
    ADDQ BX, R9
    ADDQ BX, R9

loop:
    MOVL (AX), CX

    MOVB CX, (R8)
    SHRL $8, CX

    MOVB CX, (R8)(BX*1)
    SHRL $8, CX

    MOVB CX, (R9)
    SHRL $8, CX

    MOVB CX, (R9)(BX*1)
    SHRL $8, CX

    ADDQ $4, AX
    INCQ R8
    INCQ R9
    CMPQ AX, DX
    JB loop
done:
    RET
