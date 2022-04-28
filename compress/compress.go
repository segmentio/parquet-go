// Package compress provides the generic APIs implemented by parquet compression
// codecs.
//
// https://github.com/apache/parquet-format/blob/master/Compression.md
package compress

import (
	"bytes"
	"io"
	"sync"

	"github.com/segmentio/parquet-go/format"
)

// The Codec interface represents parquet compression codecs implemented by the
// compress sub-packages.
//
// Codec instances must be safe to use concurrently from multiple goroutines.
type Codec interface {
	// Returns a human-readable name for the codec.
	String() string

	// Returns the code of the compression codec in the parquet format.
	CompressionCodec() format.CompressionCodec

	// Writes the compressed version of src to dst and returns it.
	//
	// The method automatically reallocates the output buffer if its capacity
	// was too small to hold the compressed data.
	Encode(dst, src []byte) ([]byte, error)

	// Writes the uncompressed version of src to dst and returns it.
	//
	// The method automatically reallocates the output buffer if its capacity
	// was too small to hold the uncompressed data.
	Decode(dst, src []byte) ([]byte, error)
}

type Reader interface {
	io.ReadCloser
	Reset(io.Reader) error
}

type Writer interface {
	io.WriteCloser
	Reset(io.Writer)
}

type Compressor struct {
	writers sync.Pool
}

func (c *Compressor) Encode(dst, src []byte, newWriter func(io.Writer) (Writer, error)) ([]byte, error) {
	output := bytes.NewBuffer(dst[:0])

	w, _ := c.writers.Get().(Writer)
	if w != nil {
		w.Reset(output)
	} else {
		var err error
		if w, err = newWriter(output); err != nil {
			return dst, err
		}
	}
	defer c.writers.Put(w)
	defer w.Reset(io.Discard)

	if _, err := w.Write(src); err != nil {
		return output.Bytes(), err
	}
	if err := w.Close(); err != nil {
		return output.Bytes(), err
	}
	return output.Bytes(), nil
}

type Decompressor struct {
	readers sync.Pool
}

func (d *Decompressor) Decode(dst, src []byte, newReader func(io.Reader) (Reader, error)) ([]byte, error) {
	input := bytes.NewReader(src)

	r, _ := d.readers.Get().(Reader)
	if r != nil {
		if err := r.Reset(input); err != nil {
			return dst, err
		}
	} else {
		var err error
		if r, err = newReader(input); err != nil {
			return dst, err
		}
	}

	defer func() {
		if err := r.Reset(nil); err == nil {
			d.readers.Put(r)
		}
	}()

	output := bytes.NewBuffer(dst[:0])
	_, err := output.ReadFrom(r)
	return output.Bytes(), err
}
