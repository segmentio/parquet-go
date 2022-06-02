//go:build !purego

#include "textflag.h"

// func nullIndex32bits(a array) int
TEXT ·nullIndex32bits(SB), NOSPLIT, $0-24
    MOVQ a+0(FP), AX
    MOVQ a+8(FP), BX
    XORQ SI, SI

    CMPQ BX, $0
    JE done

    CMPQ BX, $32
    JB loop1x4

    CMPB ·hasAVX2(SB), $0
    JE loop1x4

    MOVQ BX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    VPXOR Y0, Y0, Y0

loop32x4:
    VMOVDQU 0(AX)(SI*4), Y1
    VMOVDQU 32(AX)(SI*4), Y2
    VMOVDQU 64(AX)(SI*4), Y3
    VMOVDQU 96(AX)(SI*4), Y4

    VPCMPEQD Y0, Y1, Y1
    VPCMPEQD Y0, Y2, Y2
    VPCMPEQD Y0, Y3, Y3
    VPCMPEQD Y0, Y4, Y4

    VMOVMSKPS Y1, CX
    VMOVMSKPS Y2, DX
    VMOVMSKPS Y3, R8
    VMOVMSKPS Y4, R9

    SHLQ $8, DX
    SHLQ $16, R8
    SHLQ $24, R9

    ORQ DX, CX
    ORQ R8, CX
    ORQ R9, CX

    CMPL CX, $0
    JE next32x4

    TZCNTQ CX, CX
    ADDQ CX, SI
    VZEROUPPER
    JMP done

next32x4:
    ADDQ $32, SI
    CMPQ SI, DI
    JNE loop32x4

    VZEROUPPER
    CMPQ SI, BX
    JE done

loop1x4:
    MOVL (AX)(SI*4), CX
    CMPL CX, $0
    JE done
    INCQ SI
    CMPQ SI, BX
    JNE loop1x4

done:
    MOVQ SI, ret+16(FP)
    RET

// func nullIndex64bits(a array) int
TEXT ·nullIndex64bits(SB), NOSPLIT, $0-24
    MOVQ a+0(FP), AX
    MOVQ a+8(FP), BX
    XORQ SI, SI

    CMPQ BX, $0
    JE done

    CMPQ BX, $16
    JB loop1x8

    CMPB ·hasAVX2(SB), $0
    JE loop1x8

    MOVQ BX, DI
    SHRQ $4, DI
    SHLQ $4, DI
    VPXOR Y0, Y0, Y0

loop16x8:
    VMOVDQU 0(AX)(SI*8), Y1
    VMOVDQU 32(AX)(SI*8), Y2
    VMOVDQU 64(AX)(SI*8), Y3
    VMOVDQU 96(AX)(SI*8), Y4

    VPCMPEQQ Y0, Y1, Y1
    VPCMPEQQ Y0, Y2, Y2
    VPCMPEQQ Y0, Y3, Y3
    VPCMPEQQ Y0, Y4, Y4

    VMOVMSKPD Y1, CX
    VMOVMSKPD Y2, DX
    VMOVMSKPD Y3, R8
    VMOVMSKPD Y4, R9

    SHLQ $4, DX
    SHLQ $8, R8
    SHLQ $12, R9

    ORQ DX, CX
    ORQ R8, CX
    ORQ R9, CX

    CMPW CX, $0
    JE next16x8

    TZCNTQ CX, CX
    ADDQ CX, SI
    VZEROUPPER
    JMP done

next16x8:
    ADDQ $16, SI
    CMPQ SI, DI
    JNE loop16x8

    VZEROUPPER
    CMPQ SI, BX
    JE done

loop1x8:
    MOVQ (AX)(SI*8), CX
    CMPQ CX, $0
    JE done
    INCQ SI
    CMPQ SI, BX
    JNE loop1x8

done:
    MOVQ SI, ret+16(FP)
    RET

// func nullIndex128bits(a array) int
TEXT ·nullIndex128bits(SB), NOSPLIT, $0-24
    MOVQ a+0(FP), AX
    MOVQ a+8(FP), BX
    XORQ SI, SI
    PXOR X0, X0

    CMPQ BX, $0
    JE done

loop1x16:
    MOVOU (AX), X1
    PCMPEQQ X0, X1
    MOVMSKPD X1, CX

    CMPB CX, $0b11
    JE done

    ADDQ $16, AX
    INCQ SI
    CMPQ SI, BX
    JNE loop1x16

done:
    MOVQ SI, ret+16(FP)
    RET

// func nonNullIndex8bits(a array) int
TEXT ·nonNullIndex8bits(SB), NOSPLIT, $0-24
    MOVQ a+0(FP), AX
    MOVQ a+8(FP), BX
    XORQ SI, SI

    CMPQ BX, $0
    JE done

    CMPQ BX, $32
    JB loop1x1

    CMPB ·hasAVX2(SB), $0
    JE loop1x1

    MOVQ BX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    VPXOR Y0, Y0, Y0

loop32x1:
    VMOVDQU (AX)(SI*1), Y1
    VPCMPEQB Y0, Y1, Y1
    VPMOVMSKB Y1, CX

    CMPL CX, $0xFFFFFFFF
    JE next32x1

    NOTQ CX
    TZCNTQ CX, CX
    ADDQ CX, SI
    VZEROUPPER
    JMP done

next32x1:
    ADDQ $32, SI
    CMPQ SI, DI
    JNE loop32x1

    VZEROUPPER
    CMPQ SI, BX
    JE done

loop1x1:
    MOVBQZX (AX)(SI*1), CX
    CMPQ CX, $0
    JNE done
    INCQ SI
    CMPQ SI, BX
    JNE loop1x1

done:
    MOVQ SI, ret+16(FP)
    RET

// func nonNullIndex32bits(a array) int
TEXT ·nonNullIndex32bits(SB), NOSPLIT, $0-24
    MOVQ a+0(FP), AX
    MOVQ a+8(FP), BX
    XORQ SI, SI

    CMPQ BX, $0
    JE done

    CMPQ BX, $32
    JB loop1x4

    CMPB ·hasAVX2(SB), $0
    JE loop1x4

    MOVQ BX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    VPXOR Y0, Y0, Y0

loop32x4:
    VMOVDQU 0(AX)(SI*4), Y1
    VMOVDQU 32(AX)(SI*4), Y2
    VMOVDQU 64(AX)(SI*4), Y3
    VMOVDQU 96(AX)(SI*4), Y4

    VPCMPEQD Y0, Y1, Y1
    VPCMPEQD Y0, Y2, Y2
    VPCMPEQD Y0, Y3, Y3
    VPCMPEQD Y0, Y4, Y4

    VMOVMSKPS Y1, CX
    VMOVMSKPS Y2, DX
    VMOVMSKPS Y3, R8
    VMOVMSKPS Y4, R9

    SHLQ $8, DX
    SHLQ $16, R8
    SHLQ $24, R9

    ORQ DX, CX
    ORQ R8, CX
    ORQ R9, CX

    CMPL CX, $0xFFFFFFFF
    JE next32x4

    NOTQ CX
    TZCNTQ CX, CX
    ADDQ CX, SI
    VZEROUPPER
    JMP done

next32x4:
    ADDQ $32, SI
    CMPQ SI, DI
    JNE loop32x4

    VZEROUPPER
    CMPQ SI, BX
    JE done

loop1x4:
    MOVLQZX (AX)(SI*4), CX
    CMPQ CX, $0
    JNE done
    INCQ SI
    CMPQ SI, BX
    JNE loop1x4

done:
    MOVQ SI, ret+16(FP)
    RET

// func nonNullIndex64bits(a array) int
TEXT ·nonNullIndex64bits(SB), NOSPLIT, $0-24
    MOVQ a+0(FP), AX
    MOVQ a+8(FP), BX
    XORQ SI, SI

    CMPQ BX, $0
    JE done

    CMPQ BX, $16
    JB loop1x8

    CMPB ·hasAVX2(SB), $0
    JE loop1x8

    MOVQ BX, DI
    SHRQ $4, DI
    SHLQ $4, DI
    VPXOR Y0, Y0, Y0

loop16x8:
    VMOVDQU 0(AX)(SI*8), Y1
    VMOVDQU 32(AX)(SI*8), Y2
    VMOVDQU 64(AX)(SI*8), Y3
    VMOVDQU 96(AX)(SI*8), Y4

    VPCMPEQQ Y0, Y1, Y1
    VPCMPEQQ Y0, Y2, Y2
    VPCMPEQQ Y0, Y3, Y3
    VPCMPEQQ Y0, Y4, Y4

    VMOVMSKPD Y1, CX
    VMOVMSKPD Y2, DX
    VMOVMSKPD Y3, R8
    VMOVMSKPD Y4, R9

    SHLQ $4, DX
    SHLQ $8, R8
    SHLQ $12, R9

    ORQ DX, CX
    ORQ R8, CX
    ORQ R9, CX

    CMPW CX, $0xFFFF
    JE next16x8

    NOTQ CX
    TZCNTQ CX, CX
    ADDQ CX, SI
    VZEROUPPER
    JMP done

next16x8:
    ADDQ $16, SI
    CMPQ SI, DI
    JNE loop16x8

    VZEROUPPER
    CMPQ SI, BX
    JE done

loop1x8:
    MOVQ (AX)(SI*8), CX
    CMPQ CX, $0
    JNE done
    INCQ SI
    CMPQ SI, BX
    JNE loop1x8

done:
    MOVQ SI, ret+16(FP)
    RET

// func nonNullIndex128bits(a array) int
TEXT ·nonNullIndex128bits(SB), NOSPLIT, $0-24
    MOVQ a+0(FP), AX
    MOVQ a+8(FP), BX
    XORQ SI, SI
    PXOR X0, X0

    CMPQ BX, $0
    JE done

loop1x16:
    MOVOU (AX), X1
    PCMPEQQ X0, X1
    MOVMSKPD X1, CX

    CMPQ CX, $0b11
    JNE done

    ADDQ $16, AX
    INCQ SI
    CMPQ SI, BX
    JNE loop1x16

done:
    MOVQ SI, ret+16(FP)
    RET
