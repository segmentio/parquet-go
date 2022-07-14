//go:build !purego

#include "textflag.h"

// This version of the probing algorithm for 32 bit keys takes advantage of
// the memory layout of table groups and SIMD instructions to accelerate the
// probing operations.
//
// The first 32 bytes of a table group contain the bit mask indicating which
// slots are in use, and the array of keys, which fits into a single vector
// register (YMM) and can be loaded and tested with a single instruction.
//
// A first version of the table group used the number of keys held in the
// group instead of a bit mask, which required the probing operation to
// reconstruct the bit mask during the lookup operation in order to identify
// which elements of the VPCMPEQD result should be retained. The extra CPU
// instructions used to reconstruct the bit mask had a measurable overhead.
// By holding the bit mask in the data structure, we can determine the number
// of keys in a group using the POPCNT instruction, and avoid recomputing the
// mask during lookups.
//
// func multiProbe32AVX2(table []table32Group, numKeys int, hashes []uintptr, keys sparse.Uint32Array, values []int32) int
TEXT ·multiProbe32AVX2(SB), NOSPLIT, $0-112
    MOVQ table_base+0(FP), AX
    MOVQ table_len+8(FP), BX
    MOVQ numKeys+24(FP), CX
    MOVQ hashes_base+32(FP), DX
    MOVQ hashes_len+40(FP), DI
    MOVQ keys_array_ptr+56(FP), R8
    MOVQ keys_array_off+72(FP), R15
    MOVQ values_base+80(FP), R9
    DECQ BX // modulo = len(table) - 1

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R10  // hash
    VPBROADCASTD (R8), Y0 // [key]
probe:
    MOVQ R10, R11
    ANDQ BX, R11 // hash & modulo
    SHLQ $6, R11 // x 64 (size of table32Group)
    LEAQ (AX)(R11*1), R12

    VMOVDQU (R12), Y1
    VPCMPEQD Y0, Y1, Y2
    VMOVMSKPS Y2, R11
    MOVL 56(R12), R13
    TESTL R11, R13
    JZ insert

    TZCNTL R11, R13
    MOVL 28(R12)(R13*4), R14
next:
    MOVL R14, (R9)(SI*4)
    INCQ SI
    ADDQ R15, R8
test:
    CMPQ SI, DI
    JNE loop
    MOVQ CX, ret+104(FP)
    VZEROUPPER
    RET
insert:
    CMPL R13, $0b1111111
    JE probeNextGroup

    MOVL R13, R11
    POPCNTL R13, R13
    MOVQ X0, R14 // key
    SHLL $1, R11
    ORL $1, R11
    MOVL R11, 56(R12)       // group.len = (group.len << 1) | 1
    MOVL R14, (R12)(R13*4)  // group.keys[i] = key
    MOVL CX, 28(R12)(R13*4) // group.values[i] = value
    MOVL CX, R14
    INCL CX
    JMP next
probeNextGroup:
    INCQ R10
    JMP probe

// func multiProbe64AVX2(table []table64Group, numKeys int, hashes []uintptr, keys sparse.Uint64Array, values []int32) int
TEXT ·multiProbe64AVX2(SB), NOSPLIT, $0-112
    MOVQ table_base+0(FP), AX
    MOVQ table_len+8(FP), BX
    MOVQ numKeys+24(FP), CX
    MOVQ hashes_base+32(FP), DX
    MOVQ hashes_len+40(FP), DI
    MOVQ keys_array_ptr+56(FP), R8
    MOVQ keys_array_off+72(FP), R15
    MOVQ values_base+80(FP), R9
    DECQ BX // modulo = len(table) - 1

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R10        // hash
    VPBROADCASTQ (R8), Y0 // [key]
probe:
    MOVQ R10, R11
    ANDQ BX, R11 // hash & modulo
    SHLQ $6, R11 // x 64 (size of table64Group)
    LEAQ (AX)(R11*1), R12

    VMOVDQU (R12), Y1
    VPCMPEQQ Y0, Y1, Y2
    VMOVMSKPD Y2, R11
    MOVL 48(R12), R13
    TESTL R11, R13
    JZ insert

    TZCNTL R11, R13
    MOVL 32(R12)(R13*4), R14
next:
    MOVL R14, (R9)(SI*4)
    INCQ SI
    ADDQ R15, R8
test:
    CMPQ SI, DI
    JNE loop
    MOVQ CX, ret+104(FP)
    VZEROUPPER
    RET
insert:
    CMPL R13, $0b1111
    JE probeNextGroup

    MOVL R13, R11
    POPCNTL R13, R13
    SHLL $1, R11
    ORL $1, R11
    MOVL R11, 48(R12)       // group.len = (group.len << 1) | 1
    MOVQ X0, (R12)(R13*8)   // group.keys[i] = key
    MOVL CX, 32(R12)(R13*4) // group.values[i] = value
    MOVL CX, R14
    INCL CX
    JMP next
probeNextGroup:
    INCQ R10
    JMP probe

// func multiProbe128SSE2(table []byte, tableCap, tableLen int, hashes []uintptr, keys sparse.Uint128Array, values []int32) int
TEXT ·multiProbe128SSE2(SB), NOSPLIT, $0-120
    MOVQ table_base+0(FP), AX
    MOVQ tableCap+24(FP), BX
    MOVQ tableLen+32(FP), CX
    MOVQ hashes_base+40(FP), DX
    MOVQ hashes_len+48(FP), DI
    MOVQ keys_array_ptr+64(FP), R8
    MOVQ keys_array_off+80(FP), R15
    MOVQ values_base+88(FP), R9

    MOVQ BX, R10
    SHLQ $4, R10
    LEAQ (AX)(R10*1), R10
    DECQ BX // modulo = tableCap - 1

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R11 // hash
    MOVOU (R8), X0       // key
probe:
    MOVQ R11, R12
    ANDQ BX, R12

    MOVL (R10)(R12*4), R14
    CMPL R14, $0
    JE insert

    SHLQ $4, R12
    MOVOU (AX)(R12*1), X1
    PCMPEQL X0, X1
    MOVMSKPS X1, R13
    CMPL R13, $0b1111
    JE next

    INCQ R11
    JMP probe
next:
    DECL R14
    MOVL R14, (R9)(SI*4)
    INCQ SI
    ADDQ R15, R8
test:
    CMPQ SI, DI
    JNE loop
    MOVQ CX, ret+112(FP)
    RET
insert:
    INCL CX
    MOVL CX, (R10)(R12*4)
    MOVL CX, R14
    SHLQ $4, R12
    MOVOU X0, (AX)(R12*1)
    JMP next

GLOBL keymask<>(SB), RODATA|NOPTR, $32
DATA keymask<>+0(SB)/8,  $0xFFFFFFFFFFFFFFFF
DATA keymask<>+8(SB)/8,  $0xFFFFFFFFFFFFFFFF
DATA keymask<>+16(SB)/8, $0x0000000000000000
DATA keymask<>+24(SB)/8, $0x0000000000000000

// func probeStringKeyAVX2(table []stringGroup16, hash uintptr, key string, newValue int32) (value int32, insert int)
TEXT ·probeStringKeyAVX2(SB), NOSPLIT, $0-72
    MOVQ table_base+0(FP), AX
    MOVQ table_len+8(FP), BX
    MOVQ hash+24(FP), SI
    MOVQ key_base+32(FP), DX
    MOVQ key_len+40(FP), DI

    MOVL SI, R15 // slot
    DECL BX      // modulo = len(table) - 1

    MOVQ SI, X0
    MOVQ DI, X1
    VPBROADCASTB X0, X2
    VPBROADCASTB X1, X3

    MOVL $0xFFFF, R11
    MOVL DI, CX
    SUBL $16, CX
    SHRL CX, R11

    LEAQ keymask<>(SB), R9
    MOVQ DI, R10
    NEGQ R10
    VMOVDQU 16(R9)(R10*1), X4

loop:
    MOVQ R15, R8
    ANDQ BX, R8
    IMUL3Q $384, R8, R8
    LEAQ (AX)(R8*1), R8 // group = table[slot & modulo]

    VMOVDQU (R8), X0   // group.hashes
    VMOVDQU 16(R8), X1 // group.lengths
    MOVL 32(R8), R9    // group.bits

    VPCMPEQB X2, X0, X0
    VPCMPEQB X3, X1, X1
    VPAND X1, X0, X0
    VPMOVMSKB X0, R10
    ANDL R9, R10
    JZ insert

test:
    TZCNTL R10, R12
    MOVL R12, R13
    SHLL $4, R13

    VMOVDQU 128(R8)(R13*1), X0
    VMOVDQU (DX), X1
    VPAND X4, X1, X1
    VPCMPEQB X1, X0, X0
    VPMOVMSKB X0, R13
    CMPL R11, R13
    JE match

    BLSRL R10, R10
    JNZ test
    JMP insert

match:
    MOVL 64(R8)(R12*4), R8
    MOVQ R8, value+56(FP)
    MOVQ $0, insert+64(FP)
    RET

insert:
    POPCNTL R9, R9 // count
    CMPL R9, $16
    JE next

    MOVL R9, CX
    MOVL $1, R10
    SHLL CX, R10
    MOVL R10, 32(R8) // group.bits |= 1 << count

    MOVB SI, (R8)(R9*1)   // group.hashes[count] = hash
    MOVB DI, 16(R8)(R9*1) // group.lengths[count] = len(key)

    MOVL newValue+48(FP), R11
    MOVL R11, 64(R8)(R9*4)
    MOVQ R11, value+56(FP)
    MOVQ $1, insert+64(FP)

    SHLL $4, R9
    VMOVDQU (DX), X0
    VPAND X4, X0, X0
    VMOVDQU X0, 128(R8)(R9*1)
    RET

next:
    INCQ R15
    JMP loop
