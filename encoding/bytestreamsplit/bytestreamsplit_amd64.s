 //go:build !purego

#include "textflag.h"

GLOBL scale8x4<>(SB), RODATA|NOPTR, $32
DATA scale8x4<>+0(SB)/4, $0x00000000
DATA scale8x4<>+4(SB)/4, $0x00000001
DATA scale8x4<>+8(SB)/4, $0x00000002
DATA scale8x4<>+12(SB)/4, $0x00000003
DATA scale8x4<>+16(SB)/4, $0x00000000
DATA scale8x4<>+20(SB)/4, $0x00000001
DATA scale8x4<>+24(SB)/4, $0x00000002
DATA scale8x4<>+28(SB)/4, $0x00000003

GLOBL offset8x4<>(SB), RODATA|NOPTR, $32
DATA offset8x4<>+0(SB)/4, $0
DATA offset8x4<>+4(SB)/4, $0
DATA offset8x4<>+8(SB)/4, $0
DATA offset8x4<>+12(SB)/4, $0
DATA offset8x4<>+16(SB)/4, $4
DATA offset8x4<>+20(SB)/4, $4
DATA offset8x4<>+24(SB)/4, $4
DATA offset8x4<>+28(SB)/4, $4

GLOBL shuffle8x4<>(SB), RODATA|NOPTR, $32
DATA shuffle8x4<>+0(SB)/4, $0x0C080400
DATA shuffle8x4<>+4(SB)/4, $0x0D090501
DATA shuffle8x4<>+8(SB)/4, $0x0E0A0602
DATA shuffle8x4<>+12(SB)/4, $0x0F0B0703
DATA shuffle8x4<>+16(SB)/4, $0x0C080400
DATA shuffle8x4<>+20(SB)/4, $0x0D090501
DATA shuffle8x4<>+24(SB)/4, $0x0E0A0602
DATA shuffle8x4<>+28(SB)/4, $0x0F0B0703

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

    CMPQ BX, $8
    JB loop1x4

    MOVQ CX, DI
    SHRQ $5, DI
    SHLQ $5, DI

    VMOVDQU32 shuffle8x4<>(SB), Y0
    VPBROADCASTD BX, Y2
    VPMULLD scale8x4<>(SB), Y2, Y2
    VPADDD offset8x4<>(SB), Y2, Y2
loop8x4:
    KXORQ K1, K1, K1
    KNOTQ K1, K1

    VMOVDQU32 (AX), Y1
    VPSHUFB Y0, Y1, Y1
    VPSCATTERDD Y1, K1, (DX)(Y2*1)

    ADDQ $32, AX
    ADDQ $8, DX
    CMPQ AX, DI
    JNE loop8x4
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
