package bits

func Fill(b []byte, v []byte) int {
	// TODO: test whether we can produce a faster version of this routine in assembly
	n := copy(b, v)

	for i := n; i < len(b); {
		n += copy(b[i:], b[:i])
		i *= 2
	}

	return n
}
