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

#define prime1XMM X12
#define prime2XMM X13
#define prime3XMM X14
#define prime4XMM X15
#define prime5XMM X15

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

DATA prime5vec1<>+0(SB)/8, $PRIME5+1
DATA prime5vec1<>+8(SB)/8, $PRIME5+1
DATA prime5vec1<>+16(SB)/8, $PRIME5+1
DATA prime5vec1<>+24(SB)/8, $PRIME5+1
DATA prime5vec1<>+32(SB)/8, $PRIME5+1
DATA prime5vec1<>+40(SB)/8, $PRIME5+1
DATA prime5vec1<>+48(SB)/8, $PRIME5+1
DATA prime5vec1<>+56(SB)/8, $PRIME5+1
GLOBL prime5vec1<>(SB), RODATA|NOPTR, $64

DATA prime5vec2<>+0(SB)/8, $PRIME5+2
DATA prime5vec2<>+8(SB)/8, $PRIME5+2
DATA prime5vec2<>+16(SB)/8, $PRIME5+2
DATA prime5vec2<>+24(SB)/8, $PRIME5+2
DATA prime5vec2<>+32(SB)/8, $PRIME5+2
DATA prime5vec2<>+40(SB)/8, $PRIME5+2
DATA prime5vec2<>+48(SB)/8, $PRIME5+2
DATA prime5vec2<>+56(SB)/8, $PRIME5+2
GLOBL prime5vec2<>(SB), RODATA|NOPTR, $64

DATA prime5vec4<>+0(SB)/8, $PRIME5+4
DATA prime5vec4<>+8(SB)/8, $PRIME5+4
DATA prime5vec4<>+16(SB)/8, $PRIME5+4
DATA prime5vec4<>+24(SB)/8, $PRIME5+4
DATA prime5vec4<>+32(SB)/8, $PRIME5+4
DATA prime5vec4<>+40(SB)/8, $PRIME5+4
DATA prime5vec4<>+48(SB)/8, $PRIME5+4
DATA prime5vec4<>+56(SB)/8, $PRIME5+4
GLOBL prime5vec4<>(SB), RODATA|NOPTR, $64

DATA prime5vec8<>+0(SB)/8, $PRIME5+8
DATA prime5vec8<>+8(SB)/8, $PRIME5+8
DATA prime5vec8<>+16(SB)/8, $PRIME5+8
DATA prime5vec8<>+24(SB)/8, $PRIME5+8
DATA prime5vec8<>+32(SB)/8, $PRIME5+8
DATA prime5vec8<>+40(SB)/8, $PRIME5+8
DATA prime5vec8<>+48(SB)/8, $PRIME5+8
DATA prime5vec8<>+56(SB)/8, $PRIME5+8
GLOBL prime5vec8<>(SB), RODATA|NOPTR, $64

DATA prime5vec16<>+0(SB)/8, $PRIME5+16
DATA prime5vec16<>+8(SB)/8, $PRIME5+16
DATA prime5vec16<>+16(SB)/8, $PRIME5+16
DATA prime5vec16<>+24(SB)/8, $PRIME5+16
DATA prime5vec16<>+32(SB)/8, $PRIME5+16
DATA prime5vec16<>+40(SB)/8, $PRIME5+16
DATA prime5vec16<>+48(SB)/8, $PRIME5+16
DATA prime5vec16<>+56(SB)/8, $PRIME5+16
GLOBL prime5vec16<>(SB), RODATA|NOPTR, $64

DATA lowbytemask<>+0(SB)/8, $0xFF
DATA lowbytemask<>+8(SB)/8, $0xFF
DATA lowbytemask<>+16(SB)/8, $0xFF
DATA lowbytemask<>+24(SB)/8, $0xFF
DATA lowbytemask<>+32(SB)/8, $0xFF
DATA lowbytemask<>+40(SB)/8, $0xFF
DATA lowbytemask<>+48(SB)/8, $0xFF
DATA lowbytemask<>+56(SB)/8, $0xFF
GLOBL lowbytemask<>(SB), RODATA|NOPTR, $64

DATA vpermi2qeven<>+0(SB)/8, $0
DATA vpermi2qeven<>+8(SB)/8, $2
DATA vpermi2qeven<>+16(SB)/8, $4
DATA vpermi2qeven<>+24(SB)/8, $6
DATA vpermi2qeven<>+32(SB)/8, $(1<<3)|0
DATA vpermi2qeven<>+40(SB)/8, $(1<<3)|2
DATA vpermi2qeven<>+48(SB)/8, $(1<<3)|4
DATA vpermi2qeven<>+56(SB)/8, $(1<<3)|6
GLOBL vpermi2qeven<>(SB), RODATA|NOPTR, $64

DATA vpermi2qodd<>+0(SB)/8, $1
DATA vpermi2qodd<>+8(SB)/8, $3
DATA vpermi2qodd<>+16(SB)/8, $5
DATA vpermi2qodd<>+24(SB)/8, $7
DATA vpermi2qodd<>+32(SB)/8, $(1<<3)|1
DATA vpermi2qodd<>+40(SB)/8, $(1<<3)|3
DATA vpermi2qodd<>+48(SB)/8, $(1<<3)|5
DATA vpermi2qodd<>+56(SB)/8, $(1<<3)|7
GLOBL vpermi2qodd<>(SB), RODATA|NOPTR, $64

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

#define round2x64(input, acc) \
    VPMULLQ prime2XMM, input, input \
    VPADDQ input, acc, acc \
    VPROLQ $31, acc, acc \
    VPMULLQ prime1XMM, acc, acc

#define avalanche2x64(tmp, acc) \
    VPSRLQ $33, acc, tmp \
    VPXORQ tmp, acc, acc \
    VPMULLQ prime2XMM, acc, acc \
    VPSRLQ $29, acc, tmp \
    VPXORQ tmp, acc, acc \
    VPMULLQ prime3XMM, acc, acc \
    VPSRLQ $32, acc, tmp \
    VPXORQ tmp, acc, acc

#define round4x64(input, acc) \
    VPMULLQ prime2YMM, input, input \
    VPADDQ input, acc, acc \
    VPROLQ $31, acc, acc \
    VPMULLQ prime1YMM, acc, acc

#define avalanche4x64(tmp, acc) \
    VPSRLQ $33, acc, tmp \
    VPXORQ tmp, acc, acc \
    VPMULLQ prime2YMM, acc, acc \
    VPSRLQ $29, acc, tmp \
    VPXORQ tmp, acc, acc \
    VPMULLQ prime3YMM, acc, acc \
    VPSRLQ $32, acc, tmp \
    VPXORQ tmp, acc, acc

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
    CMPQ CX, $32
    JB loop
    CMPB ·hasAVX512(SB), $0
    JE loop

    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI

    VMOVDQU64 prime1vec<>(SB), prime1ZMM
    VMOVDQU64 prime2vec<>(SB), prime2ZMM
    VMOVDQU64 prime3vec<>(SB), prime3ZMM
    VMOVDQU64 prime5vec<>(SB), prime5ZMM
    VMOVDQU64 prime5vec1<>(SB), Z6
loop32x64:
    VMOVDQA64 Z6, Z0
    VMOVDQA64 Z6, Z3
    VMOVDQA64 Z6, Z20
    VMOVDQA64 Z6, Z23
    VPMOVZXBQ (BX)(SI*1), Z1
    VPMOVZXBQ 8(BX)(SI*1), Z4
    VPMOVZXBQ 16(BX)(SI*1), Z21
    VPMOVZXBQ 24(BX)(SI*1), Z24

    VPMULLQ prime5ZMM, Z1, Z1
    VPMULLQ prime5ZMM, Z4, Z4
    VPMULLQ prime5ZMM, Z21, Z21
    VPMULLQ prime5ZMM, Z24, Z24
    VPXORQ Z1, Z0, Z0
    VPXORQ Z4, Z3, Z3
    VPXORQ Z21, Z20, Z20
    VPXORQ Z24, Z23, Z23
    VPROLQ $11, Z0, Z0
    VPROLQ $11, Z3, Z3
    VPROLQ $11, Z20, Z20
    VPROLQ $11, Z23, Z23
    VPMULLQ prime1ZMM, Z0, Z0
    VPMULLQ prime1ZMM, Z3, Z3
    VPMULLQ prime1ZMM, Z20, Z20
    VPMULLQ prime1ZMM, Z23, Z23

    avalanche8x64(Z1, Z0)
    avalanche8x64(Z4, Z3)
    avalanche8x64(Z21, Z20)
    avalanche8x64(Z24, Z23)

    VMOVDQU64 Z0, (AX)(SI*8)
    VMOVDQU64 Z3, 64(AX)(SI*8)
    VMOVDQU64 Z20, 128(AX)(SI*8)
    VMOVDQU64 Z23, 192(AX)(SI*8)
    ADDQ $32, SI
    CMPQ SI, DI
    JB loop32x64
    VZEROUPPER
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
    CMPQ CX, $16
    JB loop
    CMPB ·hasAVX512(SB), $0
    JE loop

    MOVQ CX, DI
    SHRQ $4, DI
    SHLQ $4, DI

    VMOVDQU64 prime1vec<>(SB), prime1ZMM
    VMOVDQU64 prime2vec<>(SB), prime2ZMM
    VMOVDQU64 prime3vec<>(SB), prime3ZMM
    VMOVDQU64 prime5vec<>(SB), prime5ZMM
    VMOVDQU64 prime5vec2<>(SB), Z6
    VMOVDQU64 lowbytemask<>(SB), Z7
loop16x64:
    VMOVDQA64 Z6, Z0
    VMOVDQA64 Z6, Z3
    VPMOVZXWQ (BX)(SI*2), Z1
    VPMOVZXWQ 16(BX)(SI*2), Z4

    VMOVDQA64 Z1, Z8
    VMOVDQA64 Z4, Z9
    VPSRLQ $8, Z8, Z8
    VPSRLQ $8, Z9, Z9
    VPANDQ Z7, Z1, Z1
    VPANDQ Z7, Z4, Z4

    VPMULLQ prime5ZMM, Z1, Z1
    VPMULLQ prime5ZMM, Z4, Z4
    VPXORQ Z1, Z0, Z0
    VPXORQ Z4, Z3, Z3
    VPROLQ $11, Z0, Z0
    VPROLQ $11, Z3, Z3
    VPMULLQ prime1ZMM, Z0, Z0
    VPMULLQ prime1ZMM, Z3, Z3

    VPMULLQ prime5ZMM, Z8, Z8
    VPMULLQ prime5ZMM, Z9, Z9
    VPXORQ Z8, Z0, Z0
    VPXORQ Z9, Z3, Z3
    VPROLQ $11, Z0, Z0
    VPROLQ $11, Z3, Z3
    VPMULLQ prime1ZMM, Z0, Z0
    VPMULLQ prime1ZMM, Z3, Z3

    avalanche8x64(Z1, Z0)
    avalanche8x64(Z4, Z3)

    VMOVDQU64 Z0, (AX)(SI*8)
    VMOVDQU64 Z3, 64(AX)(SI*8)
    ADDQ $16, SI
    CMPQ SI, DI
    JB loop16x64
    VZEROUPPER
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
    CMPQ CX, $32
    JB loop
    CMPB ·hasAVX512(SB), $0
    JE loop

    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI

    VMOVDQU64 prime1vec<>(SB), prime1ZMM
    VMOVDQU64 prime2vec<>(SB), prime2ZMM
    VMOVDQU64 prime3vec<>(SB), prime3ZMM
    VMOVDQU64 prime5vec4<>(SB), Z6
loop32x64:
    VMOVDQA64 Z6, Z0
    VMOVDQA64 Z6, Z3
    VMOVDQA64 Z6, Z20
    VMOVDQA64 Z6, Z23
    VPMOVZXDQ (BX)(SI*4), Z1
    VPMOVZXDQ 32(BX)(SI*4), Z4
    VPMOVZXDQ 64(BX)(SI*4), Z21
    VPMOVZXDQ 96(BX)(SI*4), Z24

    VPMULLQ prime1ZMM, Z1, Z1
    VPMULLQ prime1ZMM, Z4, Z4
    VPMULLQ prime1ZMM, Z21, Z21
    VPMULLQ prime1ZMM, Z24, Z24
    VPXORQ Z1, Z0, Z0
    VPXORQ Z4, Z3, Z3
    VPXORQ Z21, Z20, Z20
    VPXORQ Z24, Z23, Z23
    VPROLQ $23, Z0, Z0
    VPROLQ $23, Z3, Z3
    VPROLQ $23, Z20, Z20
    VPROLQ $23, Z23, Z23
    VPMULLQ prime2ZMM, Z0, Z0
    VPMULLQ prime2ZMM, Z3, Z3
    VPMULLQ prime2ZMM, Z20, Z20
    VPMULLQ prime2ZMM, Z23, Z23
    VPADDQ prime3ZMM, Z0, Z0
    VPADDQ prime3ZMM, Z3, Z3
    VPADDQ prime3ZMM, Z20, Z20
    VPADDQ prime3ZMM, Z23, Z23

    avalanche8x64(Z1, Z0)
    avalanche8x64(Z4, Z3)
    avalanche8x64(Z21, Z20)
    avalanche8x64(Z24, Z23)

    VMOVDQU64 Z0, (AX)(SI*8)
    VMOVDQU64 Z3, 64(AX)(SI*8)
    VMOVDQU64 Z20, 128(AX)(SI*8)
    VMOVDQU64 Z23, 192(AX)(SI*8)
    ADDQ $32, SI
    CMPQ SI, DI
    JB loop32x64
    VZEROUPPER
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
    CMPQ CX, $32
    JB loop
    CMPB ·hasAVX512(SB), $0
    JE loop

    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI

    VMOVDQU64 prime1vec<>(SB), prime1ZMM
    VMOVDQU64 prime2vec<>(SB), prime2ZMM
    VMOVDQU64 prime3vec<>(SB), prime3ZMM
    VMOVDQU64 prime4vec<>(SB), prime4ZMM
    VMOVDQU64 prime5vec8<>(SB), Z6
loop32x64:
    VMOVDQA64 Z6, Z0
    VMOVDQA64 Z6, Z3
    VMOVDQA64 Z6, Z20
    VMOVDQA64 Z6, Z23
    VMOVDQU64 (BX)(SI*8), Z1
    VMOVDQU64 64(BX)(SI*8), Z4
    VMOVDQU64 128(BX)(SI*8), Z21
    VMOVDQU64 192(BX)(SI*8), Z24

    VPXORQ Z2, Z2, Z2
    VPXORQ Z5, Z5, Z5
    VPXORQ Z22, Z22, Z22
    VPXORQ Z25, Z25, Z25
    round8x64(Z1, Z2)
    round8x64(Z4, Z5)
    round8x64(Z21, Z22)
    round8x64(Z24, Z25)

    VPXORQ Z2, Z0, Z0
    VPXORQ Z5, Z3, Z3
    VPXORQ Z22, Z20, Z20
    VPXORQ Z25, Z23, Z23
    VPROLQ $27, Z0, Z0
    VPROLQ $27, Z3, Z3
    VPROLQ $27, Z20, Z20
    VPROLQ $27, Z23, Z23
    VPMULLQ prime1ZMM, Z0, Z0
    VPMULLQ prime1ZMM, Z3, Z3
    VPMULLQ prime1ZMM, Z20, Z20
    VPMULLQ prime1ZMM, Z23, Z23
    VPADDQ prime4ZMM, Z0, Z0
    VPADDQ prime4ZMM, Z3, Z3
    VPADDQ prime4ZMM, Z20, Z20
    VPADDQ prime4ZMM, Z23, Z23

    avalanche8x64(Z1, Z0)
    avalanche8x64(Z4, Z3)
    avalanche8x64(Z21, Z20)
    avalanche8x64(Z24, Z23)

    VMOVDQU64 Z0, (AX)(SI*8)
    VMOVDQU64 Z3, 64(AX)(SI*8)
    VMOVDQU64 Z20, 128(AX)(SI*8)
    VMOVDQU64 Z23, 192(AX)(SI*8)
    ADDQ $32, SI
    CMPQ SI, DI
    JB loop32x64
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
    CMPQ CX, $16
    JB loop
    CMPB ·hasAVX512(SB), $0
    JE loop

    MOVQ CX, DI
    SHRQ $4, DI
    SHLQ $4, DI

    VMOVDQU64 prime1vec<>(SB), prime1ZMM
    VMOVDQU64 prime2vec<>(SB), prime2ZMM
    VMOVDQU64 prime3vec<>(SB), prime3ZMM
    VMOVDQU64 prime4vec<>(SB), prime4ZMM
    VMOVDQU64 prime5vec16<>(SB), Z6
    VMOVDQU64 vpermi2qeven<>(SB), Z7
    VMOVDQU64 vpermi2qodd<>(SB), Z8
loop8x64:
    CMPQ SI, DI
    JE loop
    VMOVDQA64 Z6, Z0
    VMOVDQA64 Z6, Z20
    VMOVDQU64 (BX), Z1
    VMOVDQU64 64(BX), Z9
    VMOVDQU64 128(BX), Z21
    VMOVDQU64 192(BX), Z29

    VMOVDQA64 Z7, Z2
    VMOVDQA64 Z8, Z3
    VMOVDQA64 Z7, Z22
    VMOVDQA64 Z8, Z23

    VPERMI2Q Z9, Z1, Z2
    VPERMI2Q Z9, Z1, Z3
    VPERMI2Q Z29, Z21, Z22
    VPERMI2Q Z29, Z21, Z23

    VPXORQ Z4, Z4, Z4
    VPXORQ Z5, Z5, Z5
    VPXORQ Z24, Z24, Z24
    VPXORQ Z25, Z25, Z25
    round8x64(Z2, Z4)
    round8x64(Z3, Z5)
    round8x64(Z22, Z24)
    round8x64(Z23, Z25)

    VPXORQ Z4, Z0, Z0
    VPXORQ Z24, Z20, Z20
    VPROLQ $27, Z0, Z0
    VPROLQ $27, Z20, Z20
    VPMULLQ prime1ZMM, Z0, Z0
    VPMULLQ prime1ZMM, Z20, Z20
    VPADDQ prime4ZMM, Z0, Z0
    VPADDQ prime4ZMM, Z20, Z20

    VPXORQ Z5, Z0, Z0
    VPXORQ Z25, Z20, Z20
    VPROLQ $27, Z0, Z0
    VPROLQ $27, Z20, Z20
    VPMULLQ prime1ZMM, Z0, Z0
    VPMULLQ prime1ZMM, Z20, Z20
    VPADDQ prime4ZMM, Z0, Z0
    VPADDQ prime4ZMM, Z20, Z20

    avalanche8x64(Z1, Z0)
    avalanche8x64(Z21, Z20)
    VMOVDQU64 Z0, (AX)(SI*8)
    VMOVDQU64 Z20, 64(AX)(SI*8)
    ADDQ $256, BX
    ADDQ $16, SI
    JMP loop8x64
    VZEROUPPER
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
