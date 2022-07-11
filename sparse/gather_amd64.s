//go:build amd64 && !purego

#include "textflag.h"

// func gather128(dst [][16]byte, src Uint128Array) int
TEXT ·gather128(SB), NOSPLIT, $0-56
    MOVQ dst_base+0(FP), AX
    MOVQ dst_len+8(FP), CX
    MOVQ src_array_ptr+24(FP), BX
    MOVQ src_array_len+32(FP), DI
    MOVQ src_array_off+40(FP), DX
    XORQ SI, SI

    CMPQ DI, CX
    CMOVQLT DI, CX

    CMPQ CX, $0
    JE done

    CMPQ CX, $1
    JE tail

    XORQ SI, SI
    MOVQ CX, DI
    SHRQ $1, DI
    SHLQ $1, DI
loop:
    MOVOU (BX), X0
    MOVOU (BX)(DX*1), X1

    MOVOU X0, (AX)
    MOVOU X1, 16(AX)

    LEAQ (BX)(DX*2), BX
    ADDQ $32, AX
    ADDQ $2, SI
    CMPQ SI, DI
    JNE loop

    CMPQ SI, CX
    JE done
tail:
    MOVOU (BX), X0
    MOVOU X0, (AX)
done:
    MOVQ CX, ret+48(FP)
    RET
