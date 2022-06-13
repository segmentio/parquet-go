//go:build !purego

package delta

import "fmt"

type errno int

const (
	ok errno = iota
	invalidNegativeValueLength
	invalidNegativePrefixLength
)

const (
	padding = 64
)

func findNegativeLength(lengths []int32) int {
	for _, n := range lengths {
		if n < 0 {
			return int(n)
		}
	}
	return -1
}

func errUnknownErrorCode(e errno) error {
	return fmt.Errorf("BUG: unknown error code: %d", e)
}
