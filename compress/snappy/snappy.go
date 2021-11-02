package snappy

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/segmentio/parquet/compress"
)

type Codec struct {
}

// The snappy.Reader and snappy.Writer implement snappy encoding/decoding with
// a framing protocol, but snappy requires the implementation to use the raw
// snappy block encoding. This is why we need to use snappy.Encode/snappy.Decode
// and have to ship custom implementations of the compressed reader and writer.

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	return &reader{input: r, offset: -1}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	return &writer{output: w}, nil
}

type reader struct {
	input  io.Reader
	buffer bytes.Buffer
	offset int
	data   []byte
}

func (r *reader) Close() error {
	r.Reset(r.input)
	return nil
}

func (r *reader) Reset(rr io.Reader) error {
	r.input = rr
	r.buffer.Reset()
	r.offset = -1
	r.data = r.data[:0]
	return nil
}

func (r *reader) Read(b []byte) (int, error) {
	if r.offset < 0 {
		if r.input == nil {
			return 0, io.EOF
		}

		_, err := r.buffer.ReadFrom(r.input)
		if err != nil {
			return 0, err
		}

		r.data, err = snappy.Decode(r.data[:0], r.buffer.Bytes())
		if err != nil {
			return 0, err
		}

		r.offset = 0
	}

	n := copy(b, r.data[r.offset:])
	r.offset += n
	if r.offset == len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

type writer struct {
	output io.Writer
	buffer []byte
	data   []byte
}

func (w *writer) Close() error {
	if w.output == nil {
		w.buffer = w.buffer[:0]
		return nil
	}
	if len(w.buffer) > 0 {
		w.data = snappy.Encode(w.data[:0], w.buffer)
		w.buffer = w.buffer[:0]
	}
	_, err := w.output.Write(w.data)
	w.data = w.data[:0]
	return err
}

func (w *writer) Reset(ww io.Writer) error {
	w.output = ww
	w.buffer = w.buffer[:0]
	w.data = w.data[:0]
	return nil
}

func (w *writer) Write(b []byte) (int, error) {
	w.buffer = append(w.buffer, b...)
	return len(b), nil
}
