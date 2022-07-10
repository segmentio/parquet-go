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
// func multiProbe32AVX2(table []table32Group, numKeys int, hashes []uintptr, keys []uint32, values []int32) int
TEXT ·multiProbe32AVX2(SB), NOSPLIT, $0-112
    MOVQ table_base+0(FP), AX
    MOVQ table_len+8(FP), BX
    MOVQ numKeys+24(FP), CX
    MOVQ hashes_base+32(FP), DX
    MOVQ hashes_len+40(FP), DI
    MOVQ keys_base+56(FP), R8
    MOVQ values_base+80(FP), R9
    DECQ BX // modulo = len(table) - 1

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R10        // hash
    VPBROADCASTD (R8)(SI*4), Y0 // [key]
probe:
    MOVQ R10, R11
    ANDQ BX, R11 // hash & modulo
    SHLQ $6, R11 // x 64 (size of table32Group)
    LEAQ (AX)(R11*1), R12

    VMOVDQU (R12), Y1
    VPCMPEQD Y0, Y1, Y2
    VMOVMSKPS Y2, R11
    SHRL $1, R11
    MOVL (R12), R13
    TESTL R11, R13
    JZ insert

    TZCNTL R11, R13
    MOVL 32(R12)(R13*4), R15
next:
    MOVL R15, (R9)(SI*4)
    INCQ SI
test:
    CMPQ SI, DI
    JNE loop
    MOVQ CX, ret+104(FP)
    VZEROUPPER
    RET
insert:
    MOVL R13, R11
    POPCNTL R13, R13
    CMPL R13, $7
    JE probeNextGroup

    MOVQ X0, R14 // key
    SHLL $1, R11
    ORL $1, R11
    MOVL R11, (R12)          // group.len = (group.len << 1) | 1
    MOVL R14, 4(R12)(R13*4)  // group.keys[i] = key
    MOVL CX, 32(R12)(R13*4)  // group.values[i] = value
    MOVL CX, R15
    INCL CX
    JMP next
probeNextGroup:
    INCQ R10
    JMP probe

// func multiProbe64(table []byte, len, cap int, hashes []uintptr, keys []uint64, values []int32) int
TEXT ·multiProbe64(SB), NOSPLIT, $0-120
    MOVQ table_base+0(FP), AX
    MOVQ len+24(FP), BX
    MOVQ cap+32(FP), CX
    MOVQ hashes_base+40(FP), DX
    MOVQ hashes_len+48(FP), DI
    MOVQ keys_base+64(FP), R8
    MOVQ values_base+88(FP), R9

    MOVQ CX, R10
    SHRQ $3, R10 // offset = cap / 8

    MOVQ CX, R11
    DECQ R11 // modulo = cap - 1

    SHLQ $3, CX
    ADDQ R10, CX // offset + 8*cap

    LEAQ (AX)(R10*1), R13 // tableKeys
    LEAQ (AX)(CX*1), R10  // tableValues

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R12 // hash
probe:
    ANDQ R11, R12 // hash & modulo
    MOVQ R12, R14
    MOVQ R12, R15
    SHRQ $6, R14        // index = hash / 64
    ANDQ $0b111111, R15 // shift = hash % 64

    MOVQ (AX)(R14*8), CX
    BTSQ R15, CX
    JNC insert // tableFlags[index] & 1<<shift == 0 ?

    MOVQ (R13)(R12*8), CX
    CMPQ (R8)(SI*8), CX
    JNE nextprobe // tableKeys[hash] != keys[i] ?

    MOVL (R10)(R12*4), R14
    MOVL R14, (R9)(SI*4)
next:
    INCQ SI
test:
    CMPQ SI, DI
    JNE loop
    MOVQ BX, ret+112(FP)
    RET
insert:
    MOVQ CX, (AX)(R14*8)
    MOVQ (R8)(SI*8), R14
    MOVQ R14, (R13)(R12*8) // tableKeys[hash] = keys[i]
    MOVL BX, (R10)(R12*4)  // tableValues[hash] = len
    MOVL BX, (R9)(SI*4)    // values[i] = len
    INCQ BX                // len++
    JMP next
nextprobe:
    INCQ R12
    JMP probe

// func multiProbe128(table []byte, len, cap int, hashes []uintptr, keys [][16]byte, values []int32) int
TEXT ·multiProbe128(SB), NOSPLIT, $0-120
    MOVQ table_base+0(FP), AX
    MOVQ len+24(FP), BX
    MOVQ cap+32(FP), CX
    MOVQ hashes_base+40(FP), DX
    MOVQ hashes_len+48(FP), DI
    MOVQ keys_base+64(FP), R8
    MOVQ values_base+88(FP), R9

    MOVQ CX, R10
    SHRQ $3, R10 // offset = cap / 8

    MOVQ CX, R11
    DECQ R11 // modulo = cap - 1

    SHLQ $4, CX
    ADDQ R10, CX // offset + 16*cap

    LEAQ (AX)(R10*1), R13 // tableKeys
    LEAQ (AX)(CX*1), R10  // tableValues

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R12
probe:
    ANDQ R11, R12 // hash & modulo
    MOVQ R12, R14
    MOVQ R12, R15
    SHRQ $6, R14        // index = hash / 64
    ANDQ $0b111111, R15 // shift = hash % 64

    MOVQ (AX)(R14*8), CX
    BTSQ R15, CX
    JNC insert // tableFlags[index] & 1<<shift == 0 ?

    MOVQ R12, R14
    SHLQ $4, R14
    MOVOU (R13)(R14*1), X0
    MOVOU (R8), X1
    PCMPEQL X1, X0
    MOVMSKPS X0, R14
    CMPL R14, $0b1111
    JNE nextprobe // tableKeys[hash] != keys[i]

    MOVL (R10)(R12*4), R14
    MOVL R14, (R9)(SI*4)
next:
    ADDQ $16, R8
    INCQ SI
test:
    CMPQ SI, DI
    JNE loop
    MOVQ BX, ret+112(FP)
    RET
insert:
    MOVQ R12, R15
    SHLQ $4, R15
    MOVQ CX, (AX)(R14*8)
    MOVOU (R8), X0
    MOVOU X0, (R13)(R15*1)
    MOVL BX, (R10)(R12*4)
    MOVL BX, (R9)(SI*4)
    INCQ BX // len++
    JMP next
nextprobe:
    INCQ R12
    JMP probe
