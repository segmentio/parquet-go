// Package bitpack implements efficient bit packing and unpacking routines for
// integers of various bit widths.
package bitpack

func byteCount(bitCount uint) uint {
	return (bitCount + 7) / 8
}
