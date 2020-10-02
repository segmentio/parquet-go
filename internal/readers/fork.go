package readers

import "io"

// ForkReadSeeker is an io.ReadSeeker that can be forked.
type ForkReadSeeker interface {
	io.ReadSeeker

	// Fork returns a copy of the reader, using the same underlying reader, but
	// with its own state, which should be initialized to a copy of its
	// parent's state.
	Fork() ForkReadSeeker
}
