//go:build !purego

#include "textflag.h"

// func Hash32(value uint32, seed uintptr) uintptr
TEXT ·Hash32(SB), NOSPLIT, $0-24
    MOVL value+0(FP), AX
    MOVQ seed+8(FP), BX

    MOVOU runtime·aeskeysched+0(SB), X1
    MOVOU runtime·aeskeysched+16(SB), X2
    MOVOU runtime·aeskeysched+32(SB), X3

    MOVQ BX, X0
    PINSRD $2, AX, X0

	AESENC X1, X0
	AESENC X2, X0
	AESENC X3, X0

    MOVQ X0, ret+16(FP)
    RET

// func MultiHash32(hashes []uintptr, values []uint32, seed uintptr)
TEXT ·MultiHash32(SB), NOSPLIT, $0-56
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
    PINSRD $2, (BX)(SI*4), X0

	AESENC X1, X0
	AESENC X2, X0
	AESENC X3, X0

    MOVQ X0, (AX)(SI*8)
    INCQ SI
test:
    CMPQ SI, CX
    JNE loop
    RET

// func Hash64(value uint64, seed uintptr) uintptr
TEXT ·Hash64(SB), NOSPLIT, $0-24
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

// func MultiHash64(hashes []uintptr, values []uint64, seed uintptr)
TEXT ·MultiHash64(SB), NOSPLIT, $0-56
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
