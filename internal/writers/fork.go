package writers

import "io"

// ForkWriteSeeker is an io.WriteSeeker that can be forked.
type ForkWriteSeeker interface {
	io.WriteSeeker

	// Fork returns a copy of the reader, using the same underlying reader, but
	// with its own state, which should be initialized to a copy of its
	// parent's state.
	Fork() ForkWriteSeeker
}
