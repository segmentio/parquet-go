//go:build !purego && !amd64

#include "textflag.h"

// func minBool(data []bool) bool
TEXT ·minBool(SB), NOSPLIT, $0-25
    MOVD data_base+0(FP), R0 // data base
    MOVD data_len+8(FP), R1 // length of data
    MOVD $ret+24(FP), R2 // address for result

    MOVD $0x0101010101010101, R4 // mask

    CMP $0, R1
    BEQ false

    CMP $128, R1
    BGE loop128

loop:
    CMP $0, R1
    BEQ true

    MOVB (R0), R4
    CMP $0, R4
    BEQ false
    
    ADD $1, R0, R0
    SUB $1, R1, R1

    B loop

true:
    MOVD R0, (R2)
    RET

false:
    MOVD ZR, (R2)
    RET

loop128:
    CMP $128, R1
    BLT loop

    VLD1 (R0), [V1.D2, V2.D2]
    VAND V1.B16, V2.B16, V2.B16
    VMOV V1.D[0], R5
    VMOV V2.D[0], R6
    AND R5, R6, R6
    CMP R4, R6
    BNE false 

    ADD $128, R0, R0
    SUB $128, R1, R1
    B loop128

// func minInt32(data []int32) int32
TEXT ·minInt32(SB), NOSPLIT, $0-28
    MOVD data_base+0(FP), R0 // data base
    MOVD data_len+8(FP), R1 // length of data
    MOVD $ret+24(FP), R2 // address for result

    MOVD ZR, R3
    MOVD $0x7FFFFFFF, R4
    MOVD ZR, R6
    MOVW$4, R7

loop:
    MOVW (R6)(R0), R5
    CMP R5, R4
    CSEL LT, R4, R5, R4

    ADD $1, R3
    MUL R7, R3, R6

    CMP R3, R1
    BNE loop

done:
    MOVW R4, (R2)
    RET
