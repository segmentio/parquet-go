 //go:build !purego

#include "textflag.h"

GLOBL scale8x4<>(SB), RODATA|NOPTR, $32
DATA scale8x4<>+0(SB)/4, $0
DATA scale8x4<>+4(SB)/4, $1
DATA scale8x4<>+8(SB)/4, $2
DATA scale8x4<>+12(SB)/4, $3
DATA scale8x4<>+16(SB)/4, $0
DATA scale8x4<>+20(SB)/4, $1
DATA scale8x4<>+24(SB)/4, $2
DATA scale8x4<>+28(SB)/4, $3

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
    ADDQ BX, CX // end
    SHRQ $2, BX // len

    CMPQ BX, $0
    JE done

    CMPB ·encodeFloatHasAVX512(SB), $0
    JE loop1x4

    CMPQ BX, $8
    JB loop1x4

    MOVQ CX, DI
    SUBQ AX, DI
    SHRQ $5, DI
    SHLQ $5, DI
    ADDQ AX, DI

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
    MOVQ DX, DI

    MOVB SI, (DI)
    SHRL $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)
    SHRL $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)
    SHRL $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)

    ADDQ $4, AX
    INCQ DX
    CMPQ AX, CX
    JB loop1x4
done:
    RET

GLOBL scale8x8<>(SB), RODATA|NOPTR, $64
DATA scale8x8<>+0(SB)/8, $0
DATA scale8x8<>+8(SB)/8, $1
DATA scale8x8<>+16(SB)/8, $2
DATA scale8x8<>+24(SB)/8, $3
DATA scale8x8<>+32(SB)/8, $4
DATA scale8x8<>+40(SB)/8, $5
DATA scale8x8<>+48(SB)/8, $6
DATA scale8x8<>+56(SB)/8, $7

GLOBL shuffle8x8<>(SB), RODATA|NOPTR, $64
DATA shuffle8x8<>+0(SB)/8,  $0x3830282018100800
DATA shuffle8x8<>+8(SB)/8,  $0x3931292119110901
DATA shuffle8x8<>+16(SB)/8, $0x3A322A221A120A02
DATA shuffle8x8<>+24(SB)/8, $0x3B332B231B130B03
DATA shuffle8x8<>+32(SB)/8, $0x3C342C241C140C04
DATA shuffle8x8<>+40(SB)/8, $0x3D352D251D150D05
DATA shuffle8x8<>+48(SB)/8, $0x3E362E261E160E06
DATA shuffle8x8<>+56(SB)/8, $0x3F372F271F170F07

// func encodeDouble(dst, src []byte)
TEXT ·encodeDouble(SB), NOSPLIT, $0-48
    MOVQ src_base+24(FP), AX
    MOVQ src_len+32(FP), BX
    MOVQ dst_base+0(FP), DX

    MOVQ AX, CX
    ADDQ BX, CX
    SHRQ $3, BX

    CMPQ BX, $0
    JE done

    CMPB ·encodeDoubleHasAVX512(SB), $0
    JE loop1x8

    CMPQ BX, $8
    JB loop1x8

    MOVQ CX, DI
    SUBQ AX, DI
    SHRQ $6, DI
    SHLQ $6, DI
    ADDQ AX, DI

    VMOVDQU64 shuffle8x8<>(SB), Z0
    VPBROADCASTQ BX, Z2
    VPMULLQ scale8x8<>(SB), Z2, Z2
loop8x8:
    KXORQ K1, K1, K1
    KNOTQ K1, K1

    VMOVDQU64 (AX), Z1
    VPERMB Z1, Z0, Z1
    VPSCATTERQQ Z1, K1, (DX)(Z2*1)

    ADDQ $64, AX
    ADDQ $8, DX
    CMPQ AX, DI
    JNE loop8x8
    VZEROUPPER

    CMPQ AX, CX
    JE done
loop1x8:
    MOVQ (AX), SI
    MOVQ DX, DI

    MOVB SI, (DI)
    SHRQ $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)
    SHRQ $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)
    SHRQ $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)
    SHRQ $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)
    SHRQ $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)
    SHRQ $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)
    SHRQ $8, SI
    ADDQ BX, DI

    MOVB SI, (DI)

    ADDQ $8, AX
    INCQ DX
    CMPQ AX, CX
    JB loop1x8
done:
    RET
