package writers

import (
	"io"

	"github.com/segmentio/parquet/internal/debug"
)

// OffsetTracking is a wrapper around an io.WriteSeeker that tracks the current
// offset position for debugging purposes.
type OffsetTracking struct {
	r      io.WriteSeeker
	offset int64
}

// NewOffsetTracking creates a new OffsetTracking WriteSeeker.
func NewOffsetTracking(r io.WriteSeeker) *OffsetTracking {
	return &OffsetTracking{
		r: r,
	}
}

// Offset returns the offset of the cursor in the underlying file as tracked by
// operations made through this reader. The actual cursor might have moved.
func (otr *OffsetTracking) Offset() int64 {
	return otr.offset
}

// Read calls the underlying reader's Read method, and increment the tracked
// offset by the number of bytes that were read.
func (otr *OffsetTracking) Write(p []byte) (int, error) {
	oldOffset := otr.offset
	n, err := otr.r.Write(p)
	otr.offset += int64(n)
	debug.Format("otr: write: %d at %d (> %d)", len(p), oldOffset, otr.offset)
	return n, err
}

// Seek calls the underlying reader's Seek method, and sets the tracked offset
// accordingly.
func (otr *OffsetTracking) Seek(offset int64, whence int) (int64, error) {
	n, err := otr.r.Seek(offset, whence)
	debug.Format("otr: seek: %d > %d", otr.offset, n)
	otr.offset = n
	return n, err
}

// Fork returns a new OffsetTracking reader using the same underlying
// io.WriteSeeker. The new reader is initialize with its parent's offset.
func (otr *OffsetTracking) Fork() ForkWriteSeeker {
	return &OffsetTracking{
		r:      otr.r,
		offset: otr.Offset(),
	}
}
