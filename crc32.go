package parquet

import (
	"hash"
	"hash/crc32"
	"io"
)

type crc32Hash struct {
	hash hash.Hash32
}

func (h *crc32Hash) Reset() {
	if h.hash != nil {
		h.hash.Reset()
	}
}

func (h *crc32Hash) Sum32() uint32 {
	if h.hash != nil {
		return h.hash.Sum32()
	}
	return 0
}

func (h *crc32Hash) Write(b []byte) (int, error) {
	if h.hash == nil {
		h.hash = crc32.NewIEEE()
	}
	return h.hash.Write(b)
}

type crc32Reader struct {
	reader io.Reader
	crc32Hash
}

func (r *crc32Reader) Reset(rr io.Reader) {
	r.reader = rr
	r.crc32Hash.Reset()
}

func (r *crc32Reader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.crc32Hash.Write(b[:n])
	return n, err
}

type crc32Writer struct {
	writer io.Writer
	crc32Hash
}

func (w *crc32Writer) Reset(ww io.Writer) {
	w.writer = ww
	w.crc32Hash.Reset()
}

func (w *crc32Writer) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.crc32Hash.Write(b[:n])
	return n, err
}
