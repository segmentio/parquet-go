package bits

// Fill is an algorithm similar to the stdlib's bytes.Repeat, it writes repeated
// copies of v to b.
func Fill(b []byte, v []byte) int {
	n := copy(b, v)

	for i := n; i < len(b); {
		n += copy(b[i:], b[:i])
		i *= 2
	}

	return n
}
