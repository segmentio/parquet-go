package parquet

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// PageBufferPool is an interface abstracting the underlying implementation of
// page buffer pools.
//
// The parquet-go package provides two implementations of this interface, one
// backed by in-memory buffers (on the Go heap), and the other using temporary
// files on disk.
//
// Applications which need finer grain control over the allocation and retention
// of page buffers may choose to provide their own implementation and install it
// via the parquet.ColumnPageBuffers writer option.
//
// PageBufferPool implementations must be safe to use concurrently from multiple
// goroutines.
type PageBufferPool interface {
	// GetPageBuffer is called when a parquet writer needs to acquires a new
	// page buffer from the pool.
	GetPageBuffer() io.ReadWriteSeeker

	// PutPageBuffer is called when a parquet writer releases a page buffer to
	// the pool.
	//
	// The parquet.Writer type guarantees that the buffers it calls this method
	// with were previously acquired by a call to GetPageBuffer on the same
	// pool, and that it will not use them anymore after the call.
	PutPageBuffer(io.ReadWriteSeeker)
}

// NewPageBufferPool creates a new in-memory page buffer pool.
//
// The implementation is backed by sync.Pool and allocates memory buffers on the
// Go heap.
func NewPageBufferPool() PageBufferPool { return new(pageBufferPool) }

type pageBuffer struct {
	data []byte
	off  int
}

func (p *pageBuffer) Reset() {
	p.data, p.off = p.data[:0], 0
}

func (p *pageBuffer) Read(b []byte) (n int, err error) {
	n = copy(b, p.data[p.off:])
	p.off += n
	if p.off == len(p.data) {
		err = io.EOF
	}
	return n, err
}

func (p *pageBuffer) Write(b []byte) (int, error) {
	p.data = append(p.data, b...)
	return len(b), nil
}

func (p *pageBuffer) WriteString(s string) (int, error) {
	p.data = append(p.data, s...)
	return len(s), nil
}

func (p *pageBuffer) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(p.data[p.off:])
	p.off += n
	return int64(n), err
}

func (p *pageBuffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset += int64(p.off)
	case io.SeekEnd:
		offset += int64(len(p.data))
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek: negative offset: %d<0", offset)
	}
	if offset > int64(len(p.data)) {
		offset = int64(len(p.data))
	}
	p.off = int(offset)
	return offset, nil
}

type pageBufferPool struct{ sync.Pool }

func (pool *pageBufferPool) GetPageBuffer() io.ReadWriteSeeker {
	b, _ := pool.Get().(*pageBuffer)
	if b == nil {
		b = new(pageBuffer)
	} else {
		b.Reset()
	}
	return b
}

func (pool *pageBufferPool) PutPageBuffer(buf io.ReadWriteSeeker) {
	if b, _ := buf.(*pageBuffer); b != nil {
		pool.Put(b)
	}
}

type fileBufferPool struct {
	err     error
	tempdir string
	pattern string
}

// NewFileBufferPool creates a new on-disk page buffer pool.
func NewFileBufferPool(tempdir, pattern string) PageBufferPool {
	pool := &fileBufferPool{
		tempdir: tempdir,
		pattern: pattern,
	}
	pool.tempdir, pool.err = filepath.Abs(pool.tempdir)
	return pool
}

func (pool *fileBufferPool) GetPageBuffer() io.ReadWriteSeeker {
	if pool.err != nil {
		return &errorBuffer{err: pool.err}
	}
	f, err := os.CreateTemp(pool.tempdir, pool.pattern)
	if err != nil {
		return &errorBuffer{err: err}
	}
	return f
}

func (pool *fileBufferPool) PutPageBuffer(buf io.ReadWriteSeeker) {
	if f, _ := buf.(*os.File); f != nil {
		defer f.Close()
		os.Remove(f.Name())
	}
}

type errorBuffer struct{ err error }

func (buf *errorBuffer) Read([]byte) (int, error)          { return 0, buf.err }
func (buf *errorBuffer) Write([]byte) (int, error)         { return 0, buf.err }
func (buf *errorBuffer) WriteString(string) (int, error)   { return 0, buf.err }
func (buf *errorBuffer) ReadFrom(io.Reader) (int64, error) { return 0, buf.err }
func (buf *errorBuffer) WriteTo(io.Writer) (int64, error)  { return 0, buf.err }
func (buf *errorBuffer) Seek(int64, int) (int64, error)    { return 0, buf.err }

var (
	defaultPageBufferPool pageBufferPool

	_ io.StringWriter = (*pageBuffer)(nil)
	_ io.WriterTo     = (*pageBuffer)(nil)

	_ io.ReaderFrom   = (*errorBuffer)(nil)
	_ io.WriterTo     = (*errorBuffer)(nil)
	_ io.StringWriter = (*errorBuffer)(nil)
)
