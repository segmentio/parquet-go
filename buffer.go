package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	defaultBufferSize      = 4096
	defaultLevelBufferSize = 1024
)

var (
	defaultBufferPool bufferPool
)

var (
	ErrBufferFull = errors.New("page buffer is full")
)

type Buffer interface {
	io.Reader
	io.Writer
}

type BufferPool interface {
	GetBuffer() Buffer
	PutBuffer(Buffer)
}

func NewBufferPool() BufferPool { return new(bufferPool) }

type bufferPool struct{ sync.Pool }

func (pool *bufferPool) GetBuffer() Buffer {
	b, _ := pool.Get().(*buffer)
	if b == nil {
		b = new(buffer)
	} else {
		b.Reset()
	}
	return b
}

func (pool *bufferPool) PutBuffer(buf Buffer) {
	if b, _ := buf.(*buffer); b != nil {
		pool.Put(b)
	}
}

type buffer struct{ bytes.Buffer }

func (b *buffer) Close() error {
	b.Reset()
	return nil
}

type fileBufferPool struct {
	err     error
	tempdir string
	pattern string
}

func NewFileBufferPool(tempdir, pattern string) BufferPool {
	pool := &fileBufferPool{
		tempdir: tempdir,
		pattern: pattern,
	}
	pool.tempdir, pool.err = filepath.Abs(pool.tempdir)
	return pool
}

func (pool *fileBufferPool) GetBuffer() Buffer {
	if pool.err != nil {
		return &errorBuffer{err: pool.err}
	}
	f, err := os.CreateTemp(pool.tempdir, pool.pattern)
	if err != nil {
		return &errorBuffer{err: err}
	}
	return f
}

func (pool *fileBufferPool) PutBuffer(buf Buffer) {
	if f, _ := buf.(*os.File); f != nil {
		defer f.Close()
		os.Remove(f.Name())
	}
}

type errorBuffer struct{ err error }

func (errbuf *errorBuffer) Read([]byte) (int, error)          { return 0, errbuf.err }
func (errbuf *errorBuffer) Write([]byte) (int, error)         { return 0, errbuf.err }
func (errbuf *errorBuffer) ReadFrom(io.Reader) (int64, error) { return 0, errbuf.err }
func (errbuf *errorBuffer) WriteTo(io.Writer) (int64, error)  { return 0, errbuf.err }

var (
	_ io.ReaderFrom = (*errorBuffer)(nil)
	_ io.WriterTo   = (*errorBuffer)(nil)
)

type lengthPrefixedWriter struct {
	writer io.Writer
	buffer []byte
}

func (w *lengthPrefixedWriter) Reset(ww io.Writer) {
	w.writer = ww
	w.buffer = append(w.buffer[:0], 0, 0, 0, 0)
}

func (w *lengthPrefixedWriter) Close() error {
	if len(w.buffer) > 0 {
		defer func() { w.buffer = w.buffer[:0] }()
		binary.LittleEndian.PutUint32(w.buffer, uint32(len(w.buffer))-4)
		_, err := w.writer.Write(w.buffer)
		return err
	}
	return nil
}

func (w *lengthPrefixedWriter) Write(b []byte) (int, error) {
	w.buffer = append(w.buffer, b...)
	return len(b), nil
}
