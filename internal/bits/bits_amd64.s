//go:build !purego

#include "textflag.h"

#define bswap128lo 0x08080A0B0C0D0E0F
#define bswap128hi 0x0001020304050607

DATA bswap128+0(SB)/8, $bswap128lo
DATA bswap128+8(SB)/8, $bswap128hi
DATA bswap128+16(SB)/8, $bswap128lo
DATA bswap128+24(SB)/8, $bswap128hi
DATA bswap128+32(SB)/8, $bswap128lo
DATA bswap128+40(SB)/8, $bswap128hi
DATA bswap128+48(SB)/8, $bswap128lo
DATA bswap128+56(SB)/8, $bswap128hi
GLOBL bswap128(SB), RODATA|NOPTR, $64

DATA indexes128+0(SB)/8, $4
DATA indexes128+8(SB)/8, $5
DATA indexes128+16(SB)/8, $6
DATA indexes128+24(SB)/8, $7
DATA indexes128+32(SB)/8, $2
DATA indexes128+40(SB)/8, $3
DATA indexes128+48(SB)/8, $0
DATA indexes128+56(SB)/8, $1
GLOBL indexes128(SB), RODATA|NOPTR, $64

DATA init128+0(SB)/8, $0
DATA init128+8(SB)/8, $0
DATA init128+16(SB)/8, $1
DATA init128+24(SB)/8, $1
DATA init128+32(SB)/8, $2
DATA init128+40(SB)/8, $2
DATA init128+48(SB)/8, $3
DATA init128+56(SB)/8, $3
GLOBL init128(SB), RODATA|NOPTR, $64

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
