package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/format"
)

var (
	ErrMissingRootColumn = errors.New("parquet file is missing a root column")
)

// File represents a parquet file.
type File struct {
	metadata format.FileMetaData
	protocol thrift.CompactProtocol
	reader   io.ReaderAt
	size     int64
	buffer   [8]byte
	root     *Column
}

// OpenFile opens a parquet file from the content between offset 0 and the given
// size in r.
//
// Only the parquet magic bytes and footer are read, column chunks and other
// parts of the file are left untouched; this means that successfully opening
// a file does not validate that the pages are not corrupted.
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

	buffer := acquireBufioReader(footerData)
	defer releaseBufioReader(buffer)

	if err := thrift.NewDecoder(f.protocol.NewReader(buffer)).Decode(&f.metadata); err != nil {
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

// Root returns the root column of f.
func (f *File) Root() *Column { return f.root }

// Size returns the size of f (in bytes).
func (f *File) Size() int64 { return f.size }

// ReadAt reads bytes into b from f at the given offset.
//
// The method satisfies the io.ReaderAt interface.
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

func (f *File) readColumnIndex(chunk *format.ColumnChunk) (*ColumnIndex, error) {
	columnIndex := new(format.ColumnIndex)
	columnIndexSection := io.NewSectionReader(f.reader, chunk.ColumnIndexOffset, int64(chunk.ColumnIndexLength))

	buffer := acquireBufioReader(columnIndexSection)
	defer releaseBufioReader(buffer)

	err := thrift.NewDecoder(f.protocol.NewReader(buffer)).Decode(columnIndex)
	return (*ColumnIndex)(columnIndex), err
}

func (f *File) readOffsetIndex(chunk *format.ColumnChunk) (*OffsetIndex, error) {
	offsetIndex := new(format.OffsetIndex)
	offsetIndexSection := io.NewSectionReader(f.reader, chunk.OffsetIndexOffset, int64(chunk.OffsetIndexLength))

	buffer := acquireBufioReader(offsetIndexSection)
	defer releaseBufioReader(buffer)

	err := thrift.NewDecoder(f.protocol.NewReader(buffer)).Decode(offsetIndex)
	return (*OffsetIndex)(offsetIndex), err
}

var (
	_ io.ReaderAt = (*File)(nil)
)
