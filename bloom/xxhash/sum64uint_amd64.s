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

#define prime1YMM Y12
#define prime2YMM Y13
#define prime3YMM Y14
#define prime4YMM Y15

#define tmp1YMM Y6
#define tmp2YMM Y7
#define tmp3YMM Y8
#define tmp4YMM Y9
#define tmp5YMM Y10
#define tmp6YMM Y11

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

DATA prime5plus4vec<>+0(SB)/8, $PRIME5+4
DATA prime5plus4vec<>+8(SB)/8, $PRIME5+4
DATA prime5plus4vec<>+16(SB)/8, $PRIME5+4
DATA prime5plus4vec<>+24(SB)/8, $PRIME5+4
GLOBL prime5plus4vec<>(SB), RODATA|NOPTR, $32

DATA prime5plus8vec<>+0(SB)/8, $PRIME5+8
DATA prime5plus8vec<>+8(SB)/8, $PRIME5+8
DATA prime5plus8vec<>+16(SB)/8, $PRIME5+8
DATA prime5plus8vec<>+24(SB)/8, $PRIME5+8
GLOBL prime5plus8vec<>(SB), RODATA|NOPTR, $32

#define mulvec4x64(tmp1, tmp2, a, b, m) \
    VPSRLQ $32, b, m \
    VPSRLQ $32, a, tmp2 \
    VPMULUDQ a, m, m \
    VPMULUDQ b, tmp2, tmp2 \
    VPMULUDQ a, b, tmp1 \
    VPADDQ tmp2, m, m \
    VPSLLQ $32, m, m \
    VPADDQ m, tmp1, m

#define rotvec4x64(tmp, rot, acc) \
    VMOVDQA acc, tmp \
    VPSRLQ $(64 - rot), tmp, tmp \
    VPSLLQ $rot, acc, acc \
    VPOR tmp, acc, acc

#define round4x64(tmp1, tmp2, input, acc) \
    mulvec4x64(tmp1, tmp2, prime2YMM, input, acc) \
    VPXOR input, input, input \
    VPADDQ input, acc, input \
    rotvec4x64(tmp1, 31, input) \
    mulvec4x64(tmp1, tmp2, prime1YMM, input, acc)

#define avalanche4x64(tmp1, tmp2, tmp3, acc) \
    VMOVDQA acc, tmp1 \
    VPSRLQ $33, tmp1, tmp1 \
    VPXOR acc, tmp1, tmp1 \
    mulvec4x64(tmp2, tmp3, prime2YMM, tmp1, acc) \
    VMOVDQA acc, tmp1 \
    VPSRLQ $29, tmp1, tmp1 \
    VPXOR acc, tmp1, tmp1 \
    mulvec4x64(tmp2, tmp3, prime3YMM, tmp1, acc) \
    VMOVDQA acc, tmp1 \
    VPSRLQ $32, tmp1, tmp1 \
    VPXOR tmp1, acc, acc

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
    MOVQ CX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    CMPQ DI, $8
    JB loop

    VMOVDQA prime1vec<>(SB), prime1YMM
    VMOVDQA prime2vec<>(SB), prime2YMM
    VMOVDQA prime3vec<>(SB), prime3YMM
loop4x64:
    VMOVDQA prime5plus4vec<>(SB), Y0
    VMOVDQA prime5plus4vec<>(SB), Y3

    VMOVDQU (BX)(SI*4), X1
    VMOVDQU 16(BX)(SI*4), X4
    VPMOVZXDQ X1, Y1
    VPMOVZXDQ X4, Y4

    mulvec4x64(tmp1YMM, tmp2YMM, prime1YMM, Y1, Y2)
    mulvec4x64(tmp4YMM, tmp5YMM, prime1YMM, Y4, Y5)

    VPXOR Y2, Y0, Y0
    VPXOR Y5, Y3, Y3

    rotvec4x64(tmp1YMM, 23, Y0)
    rotvec4x64(tmp1YMM, 23, Y3)

    mulvec4x64(tmp1YMM, tmp2YMM, prime2YMM, Y0, Y1)
    mulvec4x64(tmp4YMM, tmp5YMM, prime2YMM, Y3, Y4)

    VPADDQ prime3YMM, Y1, Y1
    VPADDQ prime3YMM, Y4, Y4

    avalanche4x64(tmp1YMM, tmp2YMM, tmp3YMM, Y1)
    avalanche4x64(tmp4YMM, tmp5YMM, tmp6YMM, Y4)

    VMOVDQU Y1, (AX)(SI*8)
    VMOVDQU Y4, 32(AX)(SI*8)

    ADDQ $8, SI
    CMPQ SI, DI
    JB loop4x64
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
    MOVQ CX, DI
    SHRQ $3, DI
    SHLQ $3, DI

    CMPQ DI, $8
    JB loop

    VMOVDQA prime1vec<>(SB), prime1YMM
    VMOVDQA prime2vec<>(SB), prime2YMM
    VMOVDQA prime3vec<>(SB), prime3YMM
    VMOVDQA prime4vec<>(SB), prime4YMM
loop4x64:
    VMOVDQA prime5plus8vec<>(SB), Y0
    VMOVDQA prime5plus8vec<>(SB), Y3

    VMOVDQU (BX)(SI*8), Y1
    VMOVDQU 32(BX)(SI*8), Y4

    round4x64(tmp1YMM, tmp2YMM, Y1, Y2)
    round4x64(tmp4YMM, tmp5YMM, Y4, Y5)

    VPXOR Y2, Y0, Y0
    VPXOR Y5, Y3, Y3

    rotvec4x64(tmp1YMM, 27, Y0)
    rotvec4x64(tmp3YMM, 27, Y3)

    mulvec4x64(tmp1YMM, tmp2YMM, prime1YMM, Y0, Y1)
    mulvec4x64(tmp4YMM, tmp5YMM, prime1YMM, Y3, Y4)

    VPADDQ prime4YMM, Y1, Y1
    VPADDQ prime4YMM, Y4, Y4

    avalanche4x64(tmp1YMM, tmp2YMM, tmp3YMM, Y1)
    avalanche4x64(tmp4YMM, tmp5YMM, tmp6YMM, Y4)

    VMOVDQU Y1, (AX)(SI*8)
    VMOVDQU Y4, 32(AX)(SI*8)

    ADDQ $8, SI
    CMPQ SI, DI
    JB loop4x64
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
