//go:build !purego

#include "textflag.h"

// func addInt32(data []int32, value int32)
TEXT ·addInt32(SB), NOSPLIT, $-32
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX
    MOVL value+24(FP), BX
    XORQ SI, SI

    CMPQ CX, $0
    JE done

    CMPB ·hasAVX512(SB), $0
    JE loop

    CMPQ CX, $16
    JB loop

    MOVQ CX, DX
    SHRQ $4, DX
    SHLQ $4, DX

    VPBROADCASTD BX, Z0
loop16:
    VPADDD (AX)(SI*4), Z0, Z1
    VMOVDQU32 Z1, (AX)(SI*4)
    ADDQ $16, SI
    CMPQ SI, DX
    JNE loop16

    CMPQ SI, CX
    JE done
loop:
    ADDL BX, (AX)(SI*4)
    INCQ SI
    CMPQ SI, CX
    JNE loop
done:
    RET

// func addInt64(data []int64, value int64)
TEXT ·addInt64(SB), NOSPLIT, $-32
    MOVQ data_base+0(FP), AX
    MOVQ data_len+8(FP), CX
    MOVQ value+24(FP), BX
    XORQ SI, SI

    CMPQ CX, $0
    JE done

    CMPB ·hasAVX512(SB), $0
    JE loop

    CMPQ CX, $8
    JB loop

    MOVQ CX, DX
    SHRQ $3, DX
    SHLQ $3, DX

    VPBROADCASTQ BX, Z0
loop8:
    VPADDQ (AX)(SI*8), Z0, Z1
    VMOVDQU64 Z1, (AX)(SI*8)
    ADDQ $8, SI
    CMPQ SI, DX
    JNE loop8

    CMPQ SI, CX
    JE done
loop:
    ADDQ BX, (AX)(SI*8)
    INCQ SI
    CMPQ SI, CX
    JNE loop
done:
    RET
