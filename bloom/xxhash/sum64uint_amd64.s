//go:build !purego

#include "textflag.h"

#define PRIME1 0x9E3779B185EBCA87
#define PRIME2 0xC2B2AE3D27D4EB4F
#define PRIME3 0x165667B19E3779F9
#define PRIME4 0x85EBCA77C2B2AE63
#define PRIME5 0x27D4EB2F165667C5

#define prime1 R12
#define prime2 R13
#define prime3 R14
#define prime4 R15
#define prime5 R15 // same as prime4 because they are not used together

#define round(input, acc) \
	IMULQ prime2, input \
	ADDQ  input, acc \
	ROLQ  $31, acc \
	IMULQ prime1, acc

#define avalanche(tmp, acc) \
    MOVQ acc, tmp \
    SHRQ $33, tmp \
    XORQ tmp, acc \
    IMULQ prime2, acc \
    MOVQ acc, tmp \
    SHRQ $29, tmp \
    XORQ tmp, acc \
    IMULQ prime3, acc \
    MOVQ acc, tmp \
    SHRQ $32, tmp \
    XORQ tmp, acc

// func MultiSum64Uint8(h []uint64, v []uint8) int
TEXT ·MultiSum64Uint8(SB), NOSPLIT, $0-54
    MOVQ $PRIME1, prime1
    MOVQ $PRIME2, prime2
    MOVQ $PRIME3, prime3
    MOVQ $PRIME5, prime5

    MOVQ h_base+0(FP), AX
    MOVQ h_len+8(FP), CX
    MOVQ v_base+24(FP), BX
    MOVQ v_len+32(FP), DX

    CMPQ CX, DX
    CMOVQGT DX, CX
    MOVQ CX, ret+48(FP)

    XORQ SI, SI
loop:
    CMPQ SI, CX
    JE done
    MOVQ $PRIME5+1, R8
    MOVBQZX (BX)(SI*1), R9

    IMULQ prime5, R9
    XORQ R9, R8
    ROLQ $11, R8
    IMULQ prime1, R8
    avalanche(R9, R8)

    MOVQ R8, (AX)(SI*8)
    INCQ SI
    JMP loop
done:
    RET

// func MultiSum64Uint16(h []uint64, v []uint16) int
TEXT ·MultiSum64Uint16(SB), NOSPLIT, $0-54
    MOVQ $PRIME1, prime1
    MOVQ $PRIME2, prime2
    MOVQ $PRIME3, prime3
    MOVQ $PRIME5, prime5

    MOVQ h_base+0(FP), AX
    MOVQ h_len+8(FP), CX
    MOVQ v_base+24(FP), BX
    MOVQ v_len+32(FP), DX

    CMPQ CX, DX
    CMOVQGT DX, CX
    MOVQ CX, ret+48(FP)

    XORQ SI, SI
loop:
    CMPQ SI, CX
    JE done
    MOVQ $PRIME5+2, R8
    MOVWQZX (BX)(SI*2), R9

    MOVQ R9, R10
    SHRQ $8, R10
    ANDQ $0xFF, R9

    IMULQ prime5, R9
    XORQ R9, R8
    ROLQ $11, R8
    IMULQ prime1, R8

    IMULQ prime5, R10
    XORQ R10, R8
    ROLQ $11, R8
    IMULQ prime1, R8

    avalanche(R9, R8)

    MOVQ R8, (AX)(SI*8)
    INCQ SI
    JMP loop
done:
    RET

// func MultiSum64Uint32(h []uint64, v []uint32) int
TEXT ·MultiSum64Uint32(SB), NOSPLIT, $0-54
    MOVQ $PRIME1, prime1
    MOVQ $PRIME2, prime2
    MOVQ $PRIME3, prime3

    MOVQ h_base+0(FP), AX
    MOVQ h_len+8(FP), CX
    MOVQ v_base+24(FP), BX
    MOVQ v_len+32(FP), DX

    CMPQ CX, DX
    CMOVQGT DX, CX
    MOVQ CX, ret+48(FP)

    XORQ SI, SI
loop:
    CMPQ SI, CX
    JE done
    MOVQ $PRIME5+4, R8
    MOVLQZX (BX)(SI*4), R9

    IMULQ prime1, R9
    XORQ R9, R8
    ROLQ $23, R8
    IMULQ prime2, R8
    ADDQ prime3, R8
    avalanche(R9, R8)

    MOVQ R8, (AX)(SI*8)
    INCQ SI
    JMP loop
done:
    RET

// func MultiSum64Uint64(h []uint64, v []uint64) int
TEXT ·MultiSum64Uint64(SB), NOSPLIT, $0-54
    MOVQ $PRIME1, prime1
    MOVQ $PRIME2, prime2
    MOVQ $PRIME3, prime3
    MOVQ $PRIME4, prime4

    MOVQ h_base+0(FP), AX
    MOVQ h_len+8(FP), CX
    MOVQ v_base+24(FP), BX
    MOVQ v_len+32(FP), DX

    CMPQ CX, DX
    CMOVQGT DX, CX
    MOVQ CX, ret+48(FP)

    XORQ SI, SI
loop:
    CMPQ SI, CX
    JE done
    MOVQ $PRIME5+8, R8
    MOVQ (BX)(SI*8), R9

    XORQ R10, R10
    round(R9, R10)
    XORQ R10, R8
    ROLQ $27, R8
    IMULQ prime1, R8
    ADDQ prime4, R8
    avalanche(R9, R8)

    MOVQ R8, (AX)(SI*8)
    INCQ SI
    JMP loop
done:
    RET

// func MultiSum64Uint128(h []uint64, v [][16]byte) int
TEXT ·MultiSum64Uint128(SB), NOSPLIT, $0-54
    MOVQ $PRIME1, prime1
    MOVQ $PRIME2, prime2
    MOVQ $PRIME3, prime3
    MOVQ $PRIME4, prime4

    MOVQ h_base+0(FP), AX
    MOVQ h_len+8(FP), CX
    MOVQ v_base+24(FP), BX
    MOVQ v_len+32(FP), DX

    CMPQ CX, DX
    CMOVQGT DX, CX
    MOVQ CX, ret+48(FP)

    XORQ SI, SI
loop:
    CMPQ SI, CX
    JE done
    MOVQ $PRIME5+16, R8
    MOVQ (BX), DX
    MOVQ 8(BX), DI

    XORQ R9, R9
    XORQ R10, R10
    round(DX, R9)
    round(DI, R10)

    XORQ R9, R8
    ROLQ $27, R8
    IMULQ prime1, R8
    ADDQ prime4, R8

    XORQ R10, R8
    ROLQ $27, R8
    IMULQ prime1, R8
    ADDQ prime4, R8

    avalanche(R9, R8)
    MOVQ R8, (AX)(SI*8)
    ADDQ $16, BX
    INCQ SI
    JMP loop
done:
    RET
