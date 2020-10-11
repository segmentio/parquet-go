package readers

import (
	"io"

	"github.com/segmentio/parquet/internal/stats"
)

// ForkReadSeeker is an io.ReadSeeker that can be forked.
type ForkReadSeeker interface {
	io.ReadSeeker

	// Fork returns a copy of the reader, using the same underlying reader, but
	// with its own state, which should be initialized to a copy of its
	// parent's state.
	Fork() ForkReadSeeker

	Stats() *ReaderStats
}

type ReaderStats struct {
	Reads stats.Counter
	Seeks stats.Counter
	Forks stats.Counter
	Bytes stats.Counter
}
