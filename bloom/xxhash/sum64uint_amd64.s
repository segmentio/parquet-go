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

#define prime1YMM Y12
#define prime2YMM Y13
#define prime3YMM Y14
#define prime4YMM Y15
#define prime5YMM Y15

#define prime1ZMM Z12
#define prime2ZMM Z13
#define prime3ZMM Z14
#define prime4ZMM Z15
#define prime5ZMM Z15

DATA prime1vec<>+0(SB)/8, $PRIME1
DATA prime1vec<>+8(SB)/8, $PRIME1
DATA prime1vec<>+16(SB)/8, $PRIME1
DATA prime1vec<>+24(SB)/8, $PRIME1
DATA prime1vec<>+32(SB)/8, $PRIME1
DATA prime1vec<>+40(SB)/8, $PRIME1
DATA prime1vec<>+48(SB)/8, $PRIME1
DATA prime1vec<>+56(SB)/8, $PRIME1
GLOBL prime1vec<>(SB), RODATA|NOPTR, $64

DATA prime2vec<>+0(SB)/8, $PRIME2
DATA prime2vec<>+8(SB)/8, $PRIME2
DATA prime2vec<>+16(SB)/8, $PRIME2
DATA prime2vec<>+24(SB)/8, $PRIME2
DATA prime2vec<>+32(SB)/8, $PRIME2
DATA prime2vec<>+40(SB)/8, $PRIME2
DATA prime2vec<>+48(SB)/8, $PRIME2
DATA prime2vec<>+56(SB)/8, $PRIME2
GLOBL prime2vec<>(SB), RODATA|NOPTR, $64

DATA prime3vec<>+0(SB)/8, $PRIME3
DATA prime3vec<>+8(SB)/8, $PRIME3
DATA prime3vec<>+16(SB)/8, $PRIME3
DATA prime3vec<>+24(SB)/8, $PRIME3
DATA prime3vec<>+32(SB)/8, $PRIME3
DATA prime3vec<>+40(SB)/8, $PRIME3
DATA prime3vec<>+48(SB)/8, $PRIME3
DATA prime3vec<>+56(SB)/8, $PRIME3
GLOBL prime3vec<>(SB), RODATA|NOPTR, $64

DATA prime4vec<>+0(SB)/8, $PRIME4
DATA prime4vec<>+8(SB)/8, $PRIME4
DATA prime4vec<>+16(SB)/8, $PRIME4
DATA prime4vec<>+24(SB)/8, $PRIME4
DATA prime4vec<>+32(SB)/8, $PRIME4
DATA prime4vec<>+40(SB)/8, $PRIME4
DATA prime4vec<>+48(SB)/8, $PRIME4
DATA prime4vec<>+56(SB)/8, $PRIME4
GLOBL prime4vec<>(SB), RODATA|NOPTR, $64

DATA prime5vec<>+0(SB)/8, $PRIME5
DATA prime5vec<>+8(SB)/8, $PRIME5
DATA prime5vec<>+16(SB)/8, $PRIME5
DATA prime5vec<>+24(SB)/8, $PRIME5
DATA prime5vec<>+32(SB)/8, $PRIME5
DATA prime5vec<>+40(SB)/8, $PRIME5
DATA prime5vec<>+48(SB)/8, $PRIME5
DATA prime5vec<>+56(SB)/8, $PRIME5
GLOBL prime5vec<>(SB), RODATA|NOPTR, $64

DATA prime5vec8<>+0(SB)/8, $PRIME5+8
DATA prime5vec8<>+8(SB)/8, $PRIME5+8
DATA prime5vec8<>+16(SB)/8, $PRIME5+8
DATA prime5vec8<>+24(SB)/8, $PRIME5+8
DATA prime5vec8<>+32(SB)/8, $PRIME5+8
DATA prime5vec8<>+40(SB)/8, $PRIME5+8
DATA prime5vec8<>+48(SB)/8, $PRIME5+8
DATA prime5vec8<>+56(SB)/8, $PRIME5+8
GLOBL prime5vec8<>(SB), RODATA|NOPTR, $64

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

#define round2x4x64(input1, acc1, input2, acc2) \
    VPMULLQ prime2YMM, input1, input1 \
    VPMULLQ prime2YMM, input2, input2 \
    VPADDQ input1, acc1, acc1 \
    VPADDQ input2, acc2, acc2 \
    VPROLQ $31, acc1, acc1 \
    VPROLQ $31, acc2, acc2 \
    VPMULLQ prime1YMM, acc1, acc1 \
    VPMULLQ prime1YMM, acc2, acc2

#define avalanche2x4x64(tmp1, acc1, tmp2, acc2) \
    VPSRLQ $33, acc1, tmp1 \
    VPSRLQ $33, acc2, tmp2 \
    VPXORQ tmp1, acc1, acc1 \
    VPXORQ tmp2, acc2, acc2 \
    VPMULLQ prime2YMM, acc1, acc1 \
    VPMULLQ prime2YMM, acc2, acc2 \
    VPSRLQ $29, acc1, tmp1 \
    VPSRLQ $29, acc2, tmp2 \
    VPXORQ tmp1, acc1, acc1 \
    VPXORQ tmp2, acc2, acc2 \
    VPMULLQ prime3YMM, acc1, acc1 \
    VPMULLQ prime3YMM, acc2, acc2 \
    VPSRLQ $32, acc1, tmp1 \
    VPSRLQ $32, acc2, tmp2 \
    VPXORQ tmp1, acc1, acc1 \
    VPXORQ tmp2, acc2, acc2

#define round8x64(input, acc) \
    VPMULLQ prime2ZMM, input, input \
    VPADDQ input, acc, acc \
    VPROLQ $31, acc, acc \
    VPMULLQ prime1ZMM, acc, acc

#define avalanche8x64(tmp, acc) \
    VPSRLQ $33, acc, tmp \
    VPXORQ tmp, acc, acc \
    VPMULLQ prime2ZMM, acc, acc \
    VPSRLQ $29, acc, tmp \
    VPXORQ tmp, acc, acc \
    VPMULLQ prime3ZMM, acc, acc \
    VPSRLQ $32, acc, tmp \
    VPXORQ tmp, acc, acc

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
    CMPQ CX, $8
    JB loop

    CMPB ·hasAVX512(SB), $0
    JE loop

    MOVQ CX, DI
    SHRQ $3, DI
    SHLQ $3, DI
    
    VMOVDQU64 prime1vec<>(SB), prime1ZMM
    VMOVDQU64 prime2vec<>(SB), prime2ZMM
    VMOVDQU64 prime3vec<>(SB), prime3ZMM
    VMOVDQU64 prime4vec<>(SB), prime4ZMM
    VMOVDQU64 prime5vec8<>(SB), Z6
loop8x64:
    CMPQ SI, DI
    JE loop
    VMOVDQA64 Z6, Z0
    VMOVDQU64 (BX)(SI*8), Z1

    VPXORQ Z2, Z2, Z2
    round8x64(Z1, Z2)

    VPXORQ Z2, Z0, Z0
    VPROLQ $27, Z0, Z0
    VPMULLQ prime1ZMM, Z0, Z0
    VPADDQ prime4ZMM, Z0, Z0
    avalanche8x64(Z1, Z0)

    VMOVDQU64 Z0, (AX)(SI*8)
    ADDQ $8, SI
    JMP loop8x64
    VZEROUPPER
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
