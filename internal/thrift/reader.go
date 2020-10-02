package thrift

import (
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
	gothrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/segmentio/parquet/internal/debug"
	"github.com/segmentio/parquet/internal/readers"
)

// Reader is a ReadSeeker that allows both unmarshalling Thrift structures and
// reading directly.
// It exclusively supports compact stream Thrift transports.
type Reader struct {
	r        readers.ForkReadSeeker
	factory  *gothrift.TCompactProtocolFactory
	protocol gothrift.TProtocol
	size     int64 // sizeof the whole parquet file
}

// Unmarshalable should be respected by all structs generated by Thrift.
type Unmarshalable interface {
	Read(iprot gothrift.TProtocol) error
}

// NewReader constructs a thrift Reader from an existing io.ReadSeeker.
// No I/O is done at that point.
func NewReader(r io.ReadSeeker) *Reader {
	reader := &Reader{
		r:       readers.NewShared(r),
		factory: thrift.NewTCompactProtocolFactory(),
	}
	return reader
}

// Open the thrift file for reading.
// Seeks to the end of the file to assess its size.
// TODO: this seek will be an issue when reading an S3 file.
func (tr *Reader) Open() error {
	debug.Format("parquet.Reader: Open: end seek")
	size, err := tr.r.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("unexpected seek end error: %w", err)
	}
	debug.Format("parquet.Reader: Open: start seek")
	_, err = tr.r.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("unexpected seek start error: %w", err)
	}
	tr.size = size
	debug.Format("parquet.Reader: size: %d", tr.size)
	tr.resetProtocol()
	return nil
}

// Fork creates a copy of this Reader, with a new state, but sharing the
// underlying ReadSeeker.
func (tr *Reader) Fork() *Reader {
	return &Reader{
		r: tr.r.Fork(),
		// thrift's factory should be shareable, no need to re-create it
		factory: tr.factory,
	}
}

// Read implements the io.Reader interface, forwarding the call to the
// underlying Thrift protocol Reader.
func (tr *Reader) Read(p []byte) (int, error) {
	debug.Format("thrift: Reader: Read %d", len(p))
	return tr.protocol.Transport().Read(p)
}

// Seek implements the io.Seeker interface, which the same expectations for
// whence amd return values.
func (tr *Reader) Seek(offset int64, whence int) (int64, error) {
	n, err := tr.r.Seek(offset, whence)
	if err != nil {
		return 0, err
	}

	// seems like before every thrift read we need to make a new thrift reader
	// because it does not handle the underlying reader to seek around?
	tr.resetProtocol()
	return n, nil
}

// Unmarshal a Thrift struct from the underlying reader.
func (tr *Reader) Unmarshal(u Unmarshalable) error {
	debug.Format("thrift: Reader: unmarshal %v", u)
	err := u.Read(tr.protocol)
	return err
}

func (tr *Reader) resetProtocol() {
	debug.Format("thrift: Reader: resetProtocol")
	if tr.protocol != nil {
		err := tr.protocol.Transport().Close()
		if err != nil {
			debug.Format("thrift: Reader: resetProtocol: closing transport: %s", err)
		}
	}
	thriftReader := thrift.NewStreamTransportR(tr.r)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(tr.size))
	tr.protocol = thrift.NewTCompactProtocol(bufferReader)
}
