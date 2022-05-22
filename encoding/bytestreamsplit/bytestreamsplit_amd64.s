 //go:build !purego

#include "textflag.h"

GLOBL offsets<>(SB), RODATA|NOPTR, $16
DATA offsets<>+0(SB)/4, $0x00000000
DATA offsets<>+4(SB)/4, $0x00000001
DATA offsets<>+8(SB)/4, $0x00000002
DATA offsets<>+12(SB)/4, $0x00000003

GLOBL shuffle4x4<>(SB), RODATA|NOPTR, $16
DATA shuffle4x4<>+0(SB)/4, $0x0C080400
DATA shuffle4x4<>+4(SB)/4, $0x0D090501
DATA shuffle4x4<>+8(SB)/4, $0x0E0A0602
DATA shuffle4x4<>+12(SB)/4, $0x0F0B0703

// func encodeFloat(dst, src []byte)
TEXT ·encodeFloat(SB), NOSPLIT, $0-48
    MOVQ src_base+24(FP), AX
    MOVQ src_len+32(FP), BX
    MOVQ dst_base+0(FP), DX

    MOVQ AX, CX
    ADDQ BX, CX
    SHRQ $2, BX

    CMPQ BX, $0
    JE done

    CMPB ·hasAVX512(SB), $0
    JE loop1x4

    CMPQ BX, $4
    JB loop1x4

    MOVQ CX, DI
    SHRQ $4, DI
    SHLQ $4, DI

    VMOVDQU32 shuffle4x4<>(SB), X0
    VPBROADCASTD BX, X2
    VPMULLD offsets<>(SB), X2, X2
loop4x4:
    KXORQ K1, K1, K1
    KNOTQ K1, K1

    VMOVDQU32 (AX), X1
    VPSHUFB X0, X1, X1
    VPSCATTERDD X1, K1, (DX)(X2*1)

    ADDQ $16, AX
    ADDQ $4, DX
    CMPQ AX, DI
    JNE loop4x4
    VZEROUPPER

    CMPQ AX, CX
    JE done
loop1x4:
    MOVL (AX), SI

    MOVB SI, (DX)
    SHRL $8, SI

    MOVB SI, (DX)(BX*1)
    SHRL $8, SI

    MOVB SI, (DX)(BX*2)
    SHRL $8, SI

    LEAQ (DX)(BX*2), DI
    MOVB SI, (DI)(BX*1)

    ADDQ $4, AX
    INCQ DX
    INCQ DI
    CMPQ AX, CX
    JB loop1x4
done:
    RET
