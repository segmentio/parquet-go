//go:build !purego

#include "textflag.h"

// See block_amd64.s for a description of this algorithm.
#define generateMask(src, dst) \
    VMOVDQA ones(SB), dst \
    VPMULLD salt(SB), src, src \
    VPSRLD $27, src, src \
    VPSLLVD src, dst, dst

#define fasthash1x64(scale, value) \
    SHRQ $32, value \
    IMULQ scale, value \
    SHRQ $32, value

#define fasthash4x64(scale, value) \
    VPSRLQ $32, value, value \
    VPMULUDQ scale, value, value \
    VPSRLQ $32, value, value

// func filterInsertBulk(f []Block, x []uint64)
TEXT ·filterInsertBulk(SB), NOSPLIT, $0-48
    MOVQ f_base+0(FP), AX
    MOVQ f_len+8(FP), BX
    MOVQ x_base+24(FP), CX
    MOVQ x_len+32(FP), DI
    VPBROADCASTQ f_base+8(FP), Y9

    // Loop initialization, SI holds the current index in `x`, DI is the number
    // of elements in `x` rounded down the nearest multiple of 4.
    XORQ SI, SI
    SHRQ $2, DI
    SHLQ $2, DI
loop4x64:
    CMPQ SI, DI
    JAE loop

    // The masks and indexes for 4 input hashes are computed in each loop
    // iteration. The hashes are loaded in Y8 so we can used vetor instructions
    // to compute all 4 indexes in parallel. The lower 32 bits of the hashes are
    // also broadcasted in 4 YMM registers to compute the 4 masks that will then
    // be applied to the filter.
    VMOVDQU (CX)(SI*8), Y8
    VPBROADCASTD 0(CX)(SI*8), Y0
    VPBROADCASTD 8(CX)(SI*8), Y1
    VPBROADCASTD 16(CX)(SI*8), Y2
    VPBROADCASTD 24(CX)(SI*8), Y3

    fasthash4x64(Y9, Y8)
    generateMask(Y0, Y4)
    generateMask(Y1, Y5)
    generateMask(Y2, Y6)
    generateMask(Y3, Y7)

    // The next block of instructions move indexes from the vector to general
    // purpose registers in order to use them as offsets when applying the mask
    // to the filter.
    VPSLLQ $5, Y8, Y8        // Y8[i] *= BlockSize
    VEXTRACTI128 $1, Y8, X10 // X10 = Y8 >> 128
    MOVQ X8, R8
    VPEXTRQ $1, X8, R9
    MOVQ X10, R10
    VPEXTRQ $1, X10, R11

    // Apply masks to the filter; this operation is sensitive to aliasing, the
    // blocks overlap the CPU has to serialize the reads and writes, which has
    // a measurable impact on throughput. This would be frequent for small bloom
    // filters which may have only a few blocks, the probability of seeing
    // overlapping blocks on large filters should be small enough to make this
    // a non-issue tho.
    VPOR (AX)(R8*1), Y4, Y4
    VMOVDQU Y4, (AX)(R8*1)
    VPOR (AX)(R9*1), Y5, Y5
    VMOVDQU Y5, (AX)(R9*1)
    VPOR (AX)(R10*1), Y6, Y6
    VMOVDQU Y6, (AX)(R10*1)
    VPOR (AX)(R11*1), Y7, Y7
    VMOVDQU Y7, (AX)(R11*1)

    ADDQ $4, SI
    JMP loop4x64

loop:
    // Compute trailing elements in `x` if the length was not a multiple of 4.
    // This is the same algorthim as the one in the loop4x64 section, working
    // on a single mask/block pair at a time.
    CMPQ SI, DI
    JE done

    MOVQ (CX)(SI*8), R8
    VPBROADCASTD (CX)(SI*8), Y0

    fasthash1x64(BX, R8)
    generateMask(Y0, Y1)

    SHLQ $5, R8
    VPOR (AX)(R8*1), Y1, Y1
    VMOVDQU Y1, (AX)(R8*1)

    INCQ SI
    JMP loop

done:
    VZEROUPPER
    RET

// func filterInsert(f []Block, x uint64)
TEXT ·filterInsert(SB), NOSPLIT, $0-32
    MOVQ f_base+0(FP), AX
    MOVQ f_len+8(FP), BX
    MOVQ x+24(FP), CX
    VPBROADCASTD x+24(FP), Y1

    fasthash1x64(BX, CX)
    generateMask(Y1, Y0)
    SHLQ $5, CX

    VPOR (AX)(CX*1), Y0, Y0
    VMOVDQU Y0, (AX)(CX*1)
    VZEROUPPER
    RET

// func filterCheck(f []Block, x uint64) bool
TEXT ·filterCheck(SB), NOSPLIT, $0-33
    MOVQ f_base+0(FP), AX
    MOVQ f_len+8(FP), BX
    MOVQ x+24(FP), CX
    VPBROADCASTD x+24(FP), Y1

    fasthash1x64(BX, CX)
    generateMask(Y1, Y0)
    SHLQ $5, CX

    VPAND (AX)(CX*1), Y0, Y1
    VPTEST Y0, Y1
    SETCS ret+32(FP)
    VZEROUPPER
    RET
