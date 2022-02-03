//go:build !purego

#include "textflag.h"

DATA indexes+0(SB)/4, $8
DATA indexes+4(SB)/4, $9
DATA indexes+8(SB)/4, $10
DATA indexes+12(SB)/4, $11
DATA indexes+16(SB)/4, $12
DATA indexes+20(SB)/4, $13
DATA indexes+24(SB)/4, $14
DATA indexes+28(SB)/4, $15
DATA indexes+32(SB)/4, $4
DATA indexes+36(SB)/4, $5
DATA indexes+40(SB)/4, $6
DATA indexes+44(SB)/4, $7
DATA indexes+48(SB)/4, $2
DATA indexes+52(SB)/4, $3
DATA indexes+56(SB)/4, $0
DATA indexes+60(SB)/4, $1
GLOBL indexes(SB), RODATA|NOPTR, $64

// func minInt32(data []int32) int32
TEXT ·minInt32(SB), NOSPLIT, $-28
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX

    XORQ BX, BX
    XORQ SI, SI
    CMPQ CX, $0
    JE done
    MOVL (AX), BX

    CMPB ·hasAVX512(SB), $0
    JE loop

    CMPQ CX, $32
    JB loop
    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    VPBROADCASTD (AX), Z0
loop32:
    VMOVDQU32 (AX)(SI*4), Z1
    VMOVDQU32 64(AX)(SI*4), Z2
    VPMINSD Z1, Z0, Z0
    VPMINSD Z2, Z0, Z0
    ADDQ $32, SI
    CMPQ SI, DI
    JNE loop32

    VMOVDQU32 indexes+0(SB), Z1
    VPERMI2D Z0, Z0, Z1
    VPMINSD Y1, Y0, Y0

    VMOVDQU32 indexes+32(SB), Y1
    VPERMI2D Y0, Y0, Y1
    VPMINSD X1, X0, X0

    VMOVDQU32 indexes+48(SB), X1
    VPERMI2D X0, X0, X1
    VPMINSD X1, X0, X0
    VZEROUPPER

    MOVQ X0, DX
    MOVL DX, BX
    SHRQ $32, DX
    CMPL DX, BX
    CMOVLLT DX, BX

    CMPQ SI, CX
    JE done
loop:
    MOVL (AX)(SI*4), DX
    CMPL DX, BX
    CMOVLLT DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop
done:
    MOVL BX, ret+24(FP)
    RET

// func minInt64(data []int64) int64
TEXT ·minInt64(SB), NOSPLIT, $-32
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX

    XORQ BX, BX
    XORQ SI, SI
    CMPQ CX, $0
    JE done
    MOVQ (AX), BX

    CMPB ·hasAVX512(SB), $0
    JE loop

    CMPQ CX, $32
    JB loop
    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    VPBROADCASTQ (AX), Z0
loop32:
    VMOVDQU64 (AX)(SI*8), Z1
    VMOVDQU64 64(AX)(SI*8), Z2
    VMOVDQU64 128(AX)(SI*8), Z3
    VMOVDQU64 192(AX)(SI*8), Z4
    VPMINSQ Z1, Z2, Z5
    VPMINSQ Z3, Z4, Z6
    VPMINSQ Z5, Z6, Z1
    VPMINSQ Z1, Z0, Z0
    ADDQ $32, SI
    CMPQ SI, DI
    JNE loop32

    VMOVDQU64 indexes+0(SB), Z1
    VPERMI2D Z0, Z0, Z1
    VPMINSQ Y1, Y0, Y0

    VMOVDQU64 indexes+32(SB), Y1
    VPERMI2D Y0, Y0, Y1
    VPMINSQ X1, X0, X0

    VMOVDQU64 indexes+48(SB), X1
    VPERMI2D X0, X0, X1
    VPMINSQ X1, X0, X0
    VZEROUPPER

    MOVQ X0, BX
    CMPQ SI, CX
    JE done
loop:
    MOVQ (AX)(SI*8), DX
    CMPQ DX, BX
    CMOVQLT DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop
done:
    MOVQ BX, ret+24(FP)
    RET

// func minUint32(data []int32) int32
TEXT ·minUint32(SB), NOSPLIT, $-28
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX

    XORQ BX, BX
    XORQ SI, SI
    CMPQ CX, $0
    JE done
    MOVL (AX), BX

    CMPB ·hasAVX512(SB), $0
    JE loop

    CMPQ CX, $32
    JB loop
    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    VPBROADCASTD (AX), Z0
loop32:
    VMOVDQU32 (AX)(SI*4), Z1
    VMOVDQU32 64(AX)(SI*4), Z2
    VPMINUD Z1, Z0, Z0
    VPMINUD Z2, Z0, Z0
    ADDQ $32, SI
    CMPQ SI, DI
    JNE loop32

    VMOVDQU32 indexes+0(SB), Z1
    VPERMI2D Z0, Z0, Z1
    VPMINUD Y1, Y0, Y0

    VMOVDQU32 indexes+32(SB), Y1
    VPERMI2D Y0, Y0, Y1
    VPMINUD X1, X0, X0

    VMOVDQU32 indexes+48(SB), X1
    VPERMI2D X0, X0, X1
    VPMINUD X1, X0, X0
    VZEROUPPER

    MOVQ X0, DX
    MOVL DX, BX
    SHRQ $32, DX
    CMPL DX, BX
    CMOVLCS DX, BX

    CMPQ SI, CX
    JE done
loop:
    MOVL (AX)(SI*4), DX
    CMPL DX, BX
    CMOVLCS DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop
done:
    MOVL BX, ret+24(FP)
    RET

// func minUint64(data []uint64) uint64
TEXT ·minUint64(SB), NOSPLIT, $-32
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX

    XORQ BX, BX
    XORQ SI, SI
    CMPQ CX, $0
    JE done
    MOVQ (AX), BX

    CMPB ·hasAVX512(SB), $0
    JE loop

    CMPQ CX, $32
    JB loop
    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    VPBROADCASTQ (AX), Z0
loop32:
    VMOVDQU64 (AX)(SI*8), Z1
    VMOVDQU64 64(AX)(SI*8), Z2
    VMOVDQU64 128(AX)(SI*8), Z3
    VMOVDQU64 192(AX)(SI*8), Z4
    VPMINUQ Z1, Z2, Z5
    VPMINUQ Z3, Z4, Z6
    VPMINUQ Z5, Z6, Z1
    VPMINUQ Z1, Z0, Z0
    ADDQ $32, SI
    CMPQ SI, DI
    JNE loop32

    VMOVDQU64 indexes+0(SB), Z1
    VPERMI2D Z0, Z0, Z1
    VPMINUQ Y1, Y0, Y0

    VMOVDQU64 indexes+32(SB), Y1
    VPERMI2D Y0, Y0, Y1
    VPMINUQ X1, X0, X0

    VMOVDQU64 indexes+48(SB), X1
    VPERMI2D X0, X0, X1
    VPMINUQ X1, X0, X0
    VZEROUPPER

    MOVQ X0, BX
    CMPQ SI, CX
    JE done
loop:
    MOVQ (AX)(SI*8), DX
    CMPQ DX, BX
    CMOVQCS DX, BX
    INCQ SI
    CMPQ SI, CX
    JNE loop
done:
    MOVQ BX, ret+24(FP)
    RET

// func minFloat32(data []float32) float32
TEXT ·minFloat32(SB), NOSPLIT, $-28
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX

    XORPS X0, X0
    XORPS X1, X1
    XORQ BX, BX
    XORQ DX, DX
    XORQ SI, SI
    CMPQ CX, $0
    JE done
    MOVL (AX), BX
    MOVQ BX, X0

    CMPB ·hasAVX512(SB), $0
    JE loop

    CMPQ CX, $64
    JB loop
    MOVQ CX, DI
    SHRQ $6, DI
    SHLQ $6, DI
    VPBROADCASTD (AX), Z0
loop64:
    VMOVDQU32 (AX)(SI*4), Z1
    VMOVDQU32 64(AX)(SI*4), Z2
    VMOVDQU32 128(AX)(SI*4), Z3
    VMOVDQU32 192(AX)(SI*4), Z4
    VMINPS Z1, Z2, Z5
    VMINPS Z3, Z4, Z6
    VMINPS Z5, Z6, Z1
    VMINPS Z1, Z0, Z0
    ADDQ $64, SI
    CMPQ SI, DI
    JNE loop64

    VMOVDQU32 indexes+0(SB), Z1
    VPERMI2D Z0, Z0, Z1
    VMINPS Y1, Y0, Y0

    VMOVDQU32 indexes+32(SB), Y1
    VPERMI2D Y0, Y0, Y1
    VMINPS X1, X0, X0

    VMOVDQU32 indexes+48(SB), X1
    VPERMI2D X0, X0, X1
    VMINPS X1, X0, X0
    VZEROUPPER

    MOVAPS X0, X1
    PSRLQ $32, X1
    MOVQ X0, BX
    MOVQ X1, DX
    MOVQ BX, X0
    UCOMISS X0, X1
    CMOVLCS DX, BX

    CMPQ SI, CX
    JE done
loop:
    MOVSS (AX)(SI*4), X1
    MOVQ X1, DX
    UCOMISS X0, X1
    CMOVLCS DX, BX
    MOVQ BX, X0
    INCQ SI
    CMPQ SI, CX
    JNE loop
done:
    MOVL BX, ret+24(FP)
    RET

// func minFloat64(data []float64) float64
TEXT ·minFloat64(SB), NOSPLIT, $-32
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX

    XORPS X0, X0
    XORPS X1, X1
    XORQ BX, BX
    XORQ DX, DX
    XORQ SI, SI
    CMPQ CX, $0
    JE done
    MOVQ (AX), BX
    MOVQ BX, X0

    CMPB ·hasAVX512(SB), $0
    JE loop

    CMPQ CX, $32
    JB loop
    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    VPBROADCASTQ (AX), Z0
loop32:
    VMOVDQU64 (AX)(SI*8), Z1
    VMOVDQU64 64(AX)(SI*8), Z2
    VMOVDQU64 128(AX)(SI*8), Z3
    VMOVDQU64 192(AX)(SI*8), Z4
    VMINPD Z1, Z2, Z5
    VMINPD Z3, Z4, Z6
    VMINPD Z5, Z6, Z1
    VMINPD Z1, Z0, Z0
    ADDQ $32, SI
    CMPQ SI, DI
    JNE loop32

    VMOVDQU64 indexes+0(SB), Z1
    VPERMI2D Z0, Z0, Z1
    VMINPD Y1, Y0, Y0

    VMOVDQU64 indexes+32(SB), Y1
    VPERMI2D Y0, Y0, Y1
    VMINPD X1, X0, X0

    VMOVDQU64 indexes+48(SB), X1
    VPERMI2D X0, X0, X1
    VMINPD X1, X0, X0
    VZEROUPPER

    MOVQ X0, BX
    CMPQ SI, CX
    JE done
loop:
    MOVSD (AX)(SI*8), X1
    MOVQ X1, DX
    UCOMISD X0, X1
    CMOVQCS DX, BX
    MOVQ BX, X0
    INCQ SI
    CMPQ SI, CX
    JNE loop
done:
    MOVQ BX, ret+24(FP)
    RET

// func minBE128(data []byte) []byte
TEXT ·minBE128(SB), NOSPLIT, $-48
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX
    MOVQ CX, DX // len
    MOVQ AX, BX // min
    ADDQ AX, CX // end
    ADDQ $16, AX
    MOVQ $1, R15
    CMPQ AX, CX
    JE done
loop:
    MOVBEQQ (AX), R8
    MOVBEQQ (BX), R9
    CMPQ R8, R9
    JB less
    JA next
    MOVBEQQ 8(AX), R8
    MOVBEQQ 8(BX), R9
    CMPQ R8, R9
    JAE next
less:
    MOVQ AX, BX
next:
    ADDQ $16, AX
    CMPQ AX, CX
    JB loop
done:
    MOVQ BX, ret+24(FP)
    MOVQ $16, ret+32(FP)
    MOVQ $16, ret+40(FP)
    RET
