//go:build !purego

#include "funcdata.h"
#include "textflag.h"

// func validatePrefixAndSuffixLengthValuesAVX2(prefix, suffix []int32, maxLength int) (totalPrefixLength, totalSuffixLength int, ok bool)
TEXT ·validatePrefixAndSuffixLengthValuesAVX2(SB), NOSPLIT, $0-73

    RET
