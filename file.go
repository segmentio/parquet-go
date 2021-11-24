package parquet

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/format"
)

const (
	defaultBufferSize = 4096
)

var (
	ErrMissingRootColumn = errors.New("parquet file is missing a root column")
)

type File struct {
	metadata format.FileMetaData
	protocol thrift.CompactProtocol
	reader   io.ReaderAt
	size     int64
	buffer   [8]byte
	root     *Column
}

func OpenFile(r io.ReaderAt, size int64) (*File, error) {
	f := &File{
		reader: r,
		size:   size,
	}

	if _, err := r.ReadAt(f.buffer[:4], 0); err != nil {
		return nil, fmt.Errorf("reading magic header of parquet file: %w", err)
	}
	if string(f.buffer[:4]) != "PAR1" {
		return nil, fmt.Errorf("invalid magic header of parquet file: %q", f.buffer[:4])
	}

	if _, err := r.ReadAt(f.buffer[:8], size-8); err != nil {
		return nil, fmt.Errorf("reading magic footer of parquet file: %w", err)
	}
	if string(f.buffer[4:8]) != "PAR1" {
		return nil, fmt.Errorf("invalid magic footer of parquet file: %q", f.buffer[:4])
	}

	footerSize := int64(binary.LittleEndian.Uint32(f.buffer[:4]))
	footerData := io.NewSectionReader(r, size-(footerSize+8), footerSize)

	decoder := thrift.NewDecoder(
		f.protocol.NewReader(bufio.NewReaderSize(footerData, int(footerSize))),
	)

	if err := decoder.Decode(&f.metadata); err != nil {
		return nil, fmt.Errorf("reading parquet file metadata: %w", err)
	}

	if len(f.metadata.Schema) == 0 {
		return nil, ErrMissingRootColumn
	}

	root, err := openColumns(f)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file columns: %w", err)
	}

	f.root = root
	return f, nil
}

func (f *File) Root() *Column {
	return f.root
}

func (f *File) Size() int64 {
	return f.size
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 || off >= f.size {
		return 0, io.EOF
	}

	if limit := f.size - off; limit < int64(len(b)) {
		n, err := f.reader.ReadAt(b[:limit], off)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}

	return f.reader.ReadAt(b, off)
}

func (f *File) MetaData() *format.FileMetaData {
	return &f.metadata
}
