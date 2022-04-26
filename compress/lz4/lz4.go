// Package lz4 implements the LZ4_RAW parquet compression codec.
package lz4

import (
	"bytes"
	"io"

	"github.com/pierrec/lz4/v4"
	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/format"
)

type Level = lz4.CompressionLevel

const (
	Fast   = lz4.Fast
	Level1 = lz4.Level1
	Level2 = lz4.Level2
	Level3 = lz4.Level3
	Level4 = lz4.Level4
	Level5 = lz4.Level5
	Level6 = lz4.Level6
	Level7 = lz4.Level7
	Level8 = lz4.Level8
	Level9 = lz4.Level9
)

const (
	DefaultLevel = Fast
)

type Codec struct {
	Level Level
}

func (c *Codec) String() string {
	return "LZ4_RAW"
}

func (c *Codec) CompressionCodec() format.CompressionCodec {
	return format.Lz4Raw
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	return &reader{reader: r}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	return &writer{writer: w, compressor: lz4.CompressorHC{Level: c.Level}}, nil
}

type reader struct {
	buffer bytes.Buffer
	data   []byte
	offset int
	reader io.Reader
}

func (r *reader) Close() error {
	r.offset = len(r.data)
	r.reader = nil
	return nil
}

func (r *reader) Reset(rr io.Reader) error {
	r.buffer.Reset()
	r.data = r.data[:0]
	r.offset = 0
	r.reader = rr
	return nil
}

func (r *reader) Read(b []byte) (n int, err error) {
	if r.offset == 0 && len(r.data) == 0 {
		if err := r.decompress(); err != nil {
			return 0, err
		}
	}
	n = copy(b, r.data[r.offset:])
	r.offset += n
	if r.offset == len(r.data) {
		err = io.EOF
	}
	return n, err
}

func (r *reader) decompress() error {
	if r.reader == nil {
		return io.EOF
	}

	_, err := r.buffer.ReadFrom(r.reader)
	if err != nil {
		return err
	}

	optimisticOutputSize := 3 * r.buffer.Len()
	if cap(r.data) < optimisticOutputSize {
		r.data = make([]byte, optimisticOutputSize)
	} else {
		r.data = r.data[:cap(r.data)]
	}

	for {
		n, err := lz4.UncompressBlock(r.buffer.Bytes(), r.data)
		if err != nil {
			r.data = make([]byte, 2*len(r.data))
		} else {
			r.data = r.data[:n]
			return nil
		}
	}
}

type writer struct {
	buffer     bytes.Buffer
	data       []byte
	writer     io.Writer
	compressor lz4.CompressorHC
}

func (w *writer) Reset(ww io.Writer) error {
	w.buffer.Reset()
	w.data = w.data[:0]
	w.writer = ww
	return nil
}

func (w *writer) Write(b []byte) (int, error) {
	if w.writer == nil {
		return 0, io.ErrClosedPipe
	}
	return w.buffer.Write(b)
}

func (w *writer) Close() (err error) {
	if w.writer != nil && w.buffer.Len() > 0 {
		limit := lz4.CompressBlockBound(w.buffer.Len())
		if limit > cap(w.data) {
			w.data = make([]byte, limit)
		} else {
			w.data = w.data[:limit]
		}
		size, _ := w.compressor.CompressBlock(w.buffer.Bytes(), w.data)
		_, err = w.writer.Write(w.data[:size])
	}
	w.writer = nil
	return err
}
