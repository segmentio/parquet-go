package gzip

import (
	"compress/gzip"
	"io"

	"github.com/segmentio/parquet/compress"
)

type Codec struct {
}

func (c *Codec) NewReader(r io.Reader) (compress.Reader, error) {
	z, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return reader{z}, nil
}

func (c *Codec) NewWriter(w io.Writer) (compress.Writer, error) {
	return writer{gzip.NewWriter(w)}, nil
}

type reader struct{ *gzip.Reader }

func (r reader) Reset(rr io.Reader) error {
	if rr == nil {
		// Pass it an empty reader, which is a zero-size value implementing the
		// flate.Reader interface to avoid the construction of a bufio.Reader in
		// the call to Reset.
		rr = devNull{}
	}
	return r.Reader.Reset(rr)
}

type writer struct{ *gzip.Writer }

func (w writer) Reset(ww io.Writer) error {
	if ww == nil {
		ww = devNull{}
	}
	w.Writer.Reset(ww)
	return nil
}

type devNull struct{}

func (devNull) ReadByte() (byte, error)   { return 0, io.EOF }
func (devNull) Read([]byte) (int, error)  { return 0, io.EOF }
func (devNull) Write([]byte) (int, error) { return 0, nil }
