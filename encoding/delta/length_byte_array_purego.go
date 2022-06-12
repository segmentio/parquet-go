//go:build purego || !amd64

package delta

func decodeLengthValues(lengths []int32) (sum int, err error) {
	for _, n := range lengths {
		sum += int(n)
		if n < 0 {
			return sum, errInvalidNegativeValueLength
		}
	}
	return sum, nil
}
