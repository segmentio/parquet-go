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

DATA prime1vec<>+0(SB)/8, $PRIME1
DATA prime1vec<>+8(SB)/8, $PRIME1
DATA prime1vec<>+16(SB)/8, $PRIME1
DATA prime1vec<>+24(SB)/8, $PRIME1
GLOBL prime1vec<>(SB), RODATA|NOPTR, $32

DATA prime2vec<>+0(SB)/8, $PRIME2
DATA prime2vec<>+8(SB)/8, $PRIME2
DATA prime2vec<>+16(SB)/8, $PRIME2
DATA prime2vec<>+24(SB)/8, $PRIME2
GLOBL prime2vec<>(SB), RODATA|NOPTR, $32

DATA prime3vec<>+0(SB)/8, $PRIME3
DATA prime3vec<>+8(SB)/8, $PRIME3
DATA prime3vec<>+16(SB)/8, $PRIME3
DATA prime3vec<>+24(SB)/8, $PRIME3
GLOBL prime3vec<>(SB), RODATA|NOPTR, $32

DATA prime4vec<>+0(SB)/8, $PRIME4
DATA prime4vec<>+8(SB)/8, $PRIME4
DATA prime4vec<>+16(SB)/8, $PRIME4
DATA prime4vec<>+24(SB)/8, $PRIME4
GLOBL prime4vec<>(SB), RODATA|NOPTR, $32

DATA prime5vec<>+0(SB)/8, $PRIME5
DATA prime5vec<>+8(SB)/8, $PRIME5
DATA prime5vec<>+16(SB)/8, $PRIME5
DATA prime5vec<>+24(SB)/8, $PRIME5
GLOBL prime5vec<>(SB), RODATA|NOPTR, $32

DATA prime5add8vec<>+0(SB)/8, $PRIME5+8
DATA prime5add8vec<>+8(SB)/8, $PRIME5+8
DATA prime5add8vec<>+16(SB)/8, $PRIME5+8
DATA prime5add8vec<>+24(SB)/8, $PRIME5+8
GLOBL prime5add8vec<>(SB), RODATA|NOPTR, $32

#define round(input, acc) \
	IMULQ prime2, input \
	ADDQ  input, acc \
	ROLQ  $31, acc \
	IMULQ prime1, acc

#define avalanche(tmp, reg) \
    MOVQ reg, tmp \
    SHRQ $33, tmp \
    XORQ tmp, reg \
    IMULQ prime2, reg \
    MOVQ reg, tmp \
    SHRQ $29, tmp \
    XORQ tmp, reg \
    IMULQ prime3, reg \
    MOVQ reg, tmp \
    SHRQ $32, tmp \
    XORQ tmp, reg

// func MultiSum64Uint64(h []uint64, v []uint64) int
TEXT ·MultiSum64Uint64(SB), NOSPLIT, $0-54
    MOVQ $PRIME1, prime1
    MOVQ $PRIME2, prime2
    MOVQ $PRIME3, prime3
    MOVQ $PRIME4, prime4

    MOVQ h_base+0(FP), AX
    MOVQ h_len+8(FP), BX
    MOVQ v_base+24(FP), CX
    MOVQ v_len+32(FP), DX

    CMPQ BX, DX
    CMOVQGT DX, BX
    MOVQ BX, ret+48(FP)

    XORQ SI, SI
    MOVQ BX, DI
    SHRQ $5, DI
    SHLQ $5, DI

    CMPQ DI, $0
    JE loop

    VMOVDQA prime1vec<>(SB), Y12
    VMOVDQA prime2vec<>(SB), Y13
    VMOVDQA prime3vec<>(SB), Y14
    VMOVDQA prime4vec<>(SB), Y15
loop4x64:
    VMOVDQA prime5add8vec<>(SB), Y0
    VMOVDQU (CX)(SI*8), Y1

    VXORPD Y2, Y2, Y2

    ADDQ $32, SI
    CMPQ SI, DI
    JB loop4x64
    VZEROUPPER
loop:
    CMPQ SI, BX
    JE done

    MOVQ $PRIME5+8, R10
    MOVQ (CX)(SI*8), R9

    XORQ R8, R8
    round(R9, R8)
    XORQ R8, R10
    ROLQ $27, R10
    IMULQ prime1, R10
    ADDQ prime4, R10
    avalanche(R9, R10)

    MOVQ R10, (AX)(SI*8)

    INCQ SI
    JMP loop
done:
    RET
