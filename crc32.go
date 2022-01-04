package parquet

import (
	"hash/crc32"
	"io"
)

type crc32Reader struct {
	reader io.Reader
	crc32  uint32
}

func (r *crc32Reader) Sum32() uint32 {
	return r.crc32
}

func (r *crc32Reader) Reset(rr io.Reader) {
	r.reader = rr
	r.crc32 = 0
}

func (r *crc32Reader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.crc32 = crc32.Update(r.crc32, crc32.IEEETable, b[:n])
	return n, err
}
