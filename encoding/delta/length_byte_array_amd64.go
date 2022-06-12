//go:build !purego

package delta

import (
	"fmt"

	"golang.org/x/sys/cpu"
)

type errno int

const (
	ok errno = iota
	invalidNegativeValueLength
)

func (e errno) check() error {
	switch e {
	case ok:
		return nil
	case invalidNegativeValueLength:
		return errInvalidNegativeValueLength
	default:
		return fmt.Errorf("BUG: unknown error code: %d", e)
	}
}

//go:noescape
func decodeLengthValuesDefault(lengths []int32) (sum int, err errno)

//go:noescape
func decodeLengthValuesAVX2(lengths []int32) (sum int, err errno)

func decodeLengthValues(lengths []int32) (int, error) {
	var sum int
	var err errno
	switch {
	case cpu.X86.HasAVX2:
		sum, err = decodeLengthValuesAVX2(lengths)
	default:
		sum, err = decodeLengthValuesDefault(lengths)
	}
	return sum, err.check()
}
