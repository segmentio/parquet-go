package deprecated

import (
	"math/big"
	"math/bits"
	"unsafe"
)

// Int96 is an implementation of the deprecated INT96 parquet type.
type Int96 [3]uint32

// Negative returns true if i is a negative value.
func (i Int96) Negative() bool {
	return (i[2] >> 31) != 0
}

// Less returns true if i < j.
//
// The method implements a signed comparison between the two operands.
func (i Int96) Less(j Int96) bool {
	if i.Negative() {
		if !j.Negative() {
			return true
		}
	} else {
		if j.Negative() {
			return false
		}
	}
	for k := 2; k >= 0; k-- {
		a, b := i[k], j[k]
		switch {
		case a < b:
			return true
		case a > b:
			return false
		}
	}
	return false
}

// Int converts i to a big.Int representation.
func (i Int96) Int() *big.Int {
	z := new(big.Int)
	z.Or(z, big.NewInt(int64(int32(i[2]))))
	z.Lsh(z, 32)
	z.Or(z, big.NewInt(int64(i[1])))
	z.Lsh(z, 32)
	z.Or(z, big.NewInt(int64(i[0])))
	return z
}

// String returns a string representation of i.
func (i Int96) String() string {
	return i.Int().String()
}

// Len returns the minimum length in bits required to store the value of i.
func (i Int96) Len() int {
	n0 := bits.Len32(i[0])
	n1 := bits.Len32(i[1])
	n2 := bits.Len32(i[2])
	switch {
	case n2 != 0:
		return n2 + 64
	case n1 != 0:
		return n1 + 32
	default:
		return n0
	}
}

// Int96ToBytes convers the slice of Int96 values to a slice of bytes sharing
// the same backing array.
func Int96ToBytes(data []Int96) []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&data)), 12*len(data))
}

func MaxLenInt96(data []Int96) int {
	max := 1
	for i := range data {
		n := data[i].Len()
		if n > max {
			max = n
		}
	}
	return max
}

func MinMaxInt96(data []Int96) (min, max Int96) {
	if len(data) > 0 {
		min = data[0]
		max = data[0]
		for _, v := range data[1:] {
			if v.Less(min) {
				min = v
			}
			if max.Less(v) {
				max = v
			}
		}
	}
	return min, max
}
