package writers

import "io"

// Shared is an io.WriteSeeker that wraps an io.WriteSeeker, track its own
// offset, and assumes the position in the underling file has changed
// between each operation. This allows multiple io.WriteSeekers to use the
// same file descriptor without coordination.
//
// The obvious drawback is that it leads to a lot of seeks. For any practical
// purposes, the underlying io.WriteSeeker should be smart as to when to
// actually seek around.
//
// Shared does not provide any synchronization.
type Shared struct {
	r *OffsetTracking
}

// NewShared creates a new Shared io.WriteSeeker
func NewShared(r io.WriteSeeker) *Shared {
	return &Shared{r: NewOffsetTracking(r)}
}

// Write seeks to the currently tracked position, then calls the wrapped
// Writer's Write method.
func (s *Shared) Write(p []byte) (int, error) {
	_, err := s.r.Seek(s.r.Offset(), io.SeekStart)
	if err != nil {
		return 0, err
	}
	return s.r.Write(p)
}

// Seek is forwarded to the underlying io.WriteSeeker.
func (s *Shared) Seek(offset int64, whence int) (int64, error) {
	return s.r.Seek(offset, whence)
}

// Fork returns a new Shared reader using the same underlying io.WriteSeeker,
// but with its own independent offset tracking. The new reader is initialized
// with its parent's offset.
func (s *Shared) Fork() ForkWriteSeeker {
	return &Shared{
		r: s.r.Fork().(*OffsetTracking),
	}
}
