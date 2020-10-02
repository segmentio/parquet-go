package readers

import "io"

// Shared is an io.ReadSeeker that wraps an io.ReadSeeker, track its own
// offset, and assumes the position in the underling file has changed
// between each operation. This allows multiple io.ReadSeekers to use the
// same file descriptor without coordination.
//
// The obvious drawback is that it leads to a lot of seeks. For any practical
// purposes, the underlying io.ReadSeeker should be smart as to when to
// actually seek around.
//
// Shared does not provide any synchronization.
type Shared struct {
	r *OffsetTracking
}

// NewShared creates a new Shared io.ReadSeeker
func NewShared(r io.ReadSeeker) *Shared {
	return &Shared{r: NewOffsetTracking(r)}
}

// Read seeks to the currently tracked position, then calls the wrapped
// Reader's Read method.
func (s *Shared) Read(p []byte) (int, error) {
	_, err := s.r.Seek(s.r.Offset(), io.SeekStart)
	if err != nil {
		return 0, err
	}
	return s.r.Read(p)
}

// Seek is forwarded to the underlying io.ReadSeeker.
func (s *Shared) Seek(offset int64, whence int) (int64, error) {
	return s.r.Seek(offset, whence)
}

// Fork returns a new Shared reader using the same underlying io.ReadSeeker,
// but with its own independent offset tracking. The new reader is initialized
// with its parent's offset.
func (s *Shared) Fork() ForkReadSeeker {
	return &Shared{
		r: s.r.Fork().(*OffsetTracking),
	}
}
