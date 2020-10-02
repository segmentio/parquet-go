package buffers

func Ensure(b []byte, size int) []byte {
	if cap(b) < size {
		return make([]byte, size)
	}
	return b[:size]
}
