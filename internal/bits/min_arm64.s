//go:build !purego && !amd64

#include "textflag.h"

// func minBool(data []bool) bool
TEXT ·minBool(SB), NOSPLIT, $0-28
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

    CMP $0, R1
    BEQ zero

    MOVD R0, R6 // R6 is a copy of the first address
    MOVD $4, R5
    MUL R1, R5, R5
    ADD  R0, R5, R5 // R5 contains the address of the last value

    MOVD R1, R7 // R7 is a copy of the length of data

    MOVD (R0), R8
    VLD1 (R0), [V0.S4]

loopv8:
    CMP $8, R7
    BLT loop

    VLD1.P 32(R0), [V1.S4, V2.S4]
    VUMIN V1.S4, V0.S4, V0.S4
    VUMIN V2.S4, V0.S4, V0.S4

    SUB $8, R7
    CMP $8, R7
    BGE loopv8

    VMOV V0.S[0], R3
    VMOV V0.S[1], R4

    CMP R3, R4
    CSEL LT, R4, R3, R4

    VMOV V0.S[2], R3
    CMP R3, R4
    CSEL LT, R4, R3, R4

    VMOV V0.S[3], R3
    CMP R3, R4
    CSEL LT, R4, R3, R4

    CMP R4, R8
    CSEL LT, R8, R4, R8

loop:
    CMP R0, R5
    BEQ done

    MOVW.P 4(R0), R3
    CMP R3, R8
    CSEL LT, R8, R3, R8

    BNE loop

done:
    MOVW R8, (R2)
    RET

zero:
    MOVD ZR, (R2)
    RET
