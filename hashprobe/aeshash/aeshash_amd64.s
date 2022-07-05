//go:build !purego

#include "textflag.h"

// func Sum32Uint32(value, seed uint32) uint32
TEXT ·Sum32Uint32(SB), NOSPLIT, $0-12
    MOVL value+0(FP), AX
    MOVL seed+4(FP), BX

    MOVOU runtime·aeskeysched+0(SB), X1
    MOVOU runtime·aeskeysched+16(SB), X2
    MOVOU runtime·aeskeysched+32(SB), X3

    MOVQ BX, X0
    PINSRD $2, AX, X0

	AESENC X1, X0
	AESENC X2, X0
	AESENC X3, X0

    MOVQ X0, AX
    MOVL AX, ret+8(FP)
    RET

// func MultiSum32Uint32(hashes, values []uint32, seed uint32)
TEXT ·MultiSum32Uint32(SB), NOSPLIT, $0-52
    MOVQ hashes_base+0(FP), AX
    MOVQ values_base+24(FP), BX
    MOVQ values_len+32(FP), CX
    MOVL seed+48(FP), DX

    MOVOU runtime·aeskeysched+0(SB), X1
    MOVOU runtime·aeskeysched+16(SB), X2
    MOVOU runtime·aeskeysched+32(SB), X3

    XORQ SI, SI
    JMP test
loop:
    MOVQ DX, X0
    PINSRD $2, (BX)(SI*4), X0

	AESENC X1, X0
	AESENC X2, X0
	AESENC X3, X0

    MOVQ X0, DI
    MOVL DI, (AX)(SI*4)
    INCQ SI
test:
    CMPQ SI, CX
    JNE loop
    RET

// func Sum64Uint64(value, seed uint64) uint64
TEXT ·Sum64Uint64(SB), NOSPLIT, $0-24
    MOVQ value+0(FP), AX
    MOVQ seed+8(FP), BX

    MOVOU runtime·aeskeysched+0(SB), X1
    MOVOU runtime·aeskeysched+16(SB), X2
    MOVOU runtime·aeskeysched+32(SB), X3

    MOVQ BX, X0
    PINSRQ $1, AX, X0

	AESENC X1, X0
	AESENC X2, X0
	AESENC X3, X0

    MOVQ X0, ret+16(FP)
    RET

// func MultiSum64Uint64(hashes, values []uint64, seed uint64)
TEXT ·MultiSum64Uint64(SB), NOSPLIT, $0-56
    MOVQ hashes_base+0(FP), AX
    MOVQ values_base+24(FP), BX
    MOVQ values_len+32(FP), CX
    MOVQ seed+48(FP), DX

    MOVOU runtime·aeskeysched+0(SB), X1
    MOVOU runtime·aeskeysched+16(SB), X2
    MOVOU runtime·aeskeysched+32(SB), X3

    XORQ SI, SI
    JMP test
loop:
    MOVQ DX, X0
    PINSRQ $1, (BX)(SI*8), X0

	AESENC X1, X0
	AESENC X2, X0
	AESENC X3, X0

    MOVQ X0, (AX)(SI*8)
    INCQ SI
test:
    CMPQ SI, CX
    JNE loop
    RET
