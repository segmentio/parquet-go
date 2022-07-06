//go:build !purego

#include "textflag.h"

// func multiProbe32(table []uint32, len, cap int, hashes []uintptr, keys []uint32, values []int32) int
TEXT ·multiProbe32(SB), NOSPLIT, $0-120
    MOVQ table_base+0(FP), AX
    MOVQ len+24(FP), BX
    MOVQ cap+32(FP), CX
    MOVQ hashes_base+40(FP), DX
    MOVQ hashes_len+48(FP), DI
    MOVQ keys_base+64(FP), R8
    MOVQ values_base+88(FP), R9

    MOVQ CX, R10
    SHRQ $5, R10 // offset = cap / 32

    MOVQ CX, R11
    DECQ R11 // modulo = cap - 1

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R12 // hash
probe:
    MOVQ R12, R13
    ANDQ R11, R13 // position = hash & modulo

    MOVQ R13, R14
    MOVQ R13, R15
    SHRQ $5, R14       // index = position / 32
    ANDQ $0b11111, R15 // shift = position % 32

    SHLQ $1, R13  // position *= 2
    ADDQ R10, R13 // position += offset

    MOVL (AX)(R14*4), CX
    BTSL R15, CX
    JNC insert // table[index] & 1<<shift == 0 ?

    MOVL (AX)(R13*4), CX
    CMPL (R8)(SI*4), CX
    JNE nextprobe // table[position] != keys[i] ?
    MOVL 4(AX)(R13*4), R13
    MOVL R13, (R9)(SI*4)
next:
    INCQ SI
test:
    CMPQ SI, DI
    JNE loop
    MOVQ BX, ret+112(FP)
    RET
insert:
    MOVL CX, (AX)(R14*4)
    MOVL (R8)(SI*4), R14 // key
    MOVL R14, (AX)(R13*4)
    MOVL BX, 4(AX)(R13*4)
    MOVL BX, (R9)(SI*4)
    INCQ BX // len++
    JMP next
nextprobe:
    INCQ R12
    JMP probe

// func multiProbe64(table []uint64, len, cap int, hashes []uintptr, keys []uint64, values []int32) int
TEXT ·multiProbe64(SB), NOSPLIT, $0-120
    MOVQ table_base+0(FP), AX
    MOVQ len+24(FP), BX
    MOVQ cap+32(FP), CX
    MOVQ hashes_base+40(FP), DX
    MOVQ hashes_len+48(FP), DI
    MOVQ keys_base+64(FP), R8
    MOVQ values_base+88(FP), R9

    MOVQ CX, R10
    SHRQ $6, R10 // offset = cap / 64

    MOVQ CX, R11
    DECQ R11 // modulo = cap - 1

    XORQ SI, SI
    JMP test
loop:
    MOVQ (DX)(SI*8), R12 // hash
probe:
    MOVQ R12, R13
    ANDQ R11, R13 // position = hash & modulo

    MOVQ R13, R14
    MOVQ R13, R15
    SHRQ $6, R14        // index = position / 64
    ANDQ $0b111111, R15 // shift = position % 64

    SHLQ $1, R13  // position *= 2
    ADDQ R10, R13 // position += offset

    MOVQ (AX)(R14*8), CX
    BTSQ R15, CX
    JNC insert // table[index] & 1<<shift == 0 ?

    MOVQ (AX)(R13*8), CX
    CMPQ (R8)(SI*8), CX
    JNE nextprobe // table[position] != keys[i] ?
    MOVL 8(AX)(R13*8), R13
    MOVL R13, (R9)(SI*4)
next:
    INCQ SI
test:
    CMPQ SI, DI
    JNE loop
    MOVQ BX, ret+112(FP)
    RET
insert:
    MOVQ CX, (AX)(R14*8)
    MOVQ (R8)(SI*8), R14 // key
    MOVQ R14, (AX)(R13*8)
    MOVQ BX, 8(AX)(R13*8)
    MOVL BX, (R9)(SI*4)
    INCQ BX // len++
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
    JNE nextprobe // tableKeys[hash] == keys[i]

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
