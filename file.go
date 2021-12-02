package parquet

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/format"
)

var (
	ErrMissingRootColumn = errors.New("parquet file is missing a root column")
)

// File represents a parquet file.
type File struct {
	metadata      format.FileMetaData
	protocol      thrift.CompactProtocol
	reader        io.ReaderAt
	size          int64
	root          *Column
	columnIndexes []ColumnIndex
	offsetIndexes []OffsetIndex
}

// OpenFile opens a parquet file from the content between offset 0 and the given
// size in r.
//
// Only the parquet magic bytes and footer are read, column chunks and other
// parts of the file are left untouched; this means that successfully opening
// a file does not validate that the pages are not corrupted.
func OpenFile(r io.ReaderAt, size int64, options ...FileOption) (*File, error) {
	b := make([]byte, 8)
	f := &File{reader: r, size: size}
	c := &FileConfig{}
	c.Apply(options...)

	if err := c.Validate(); err != nil {
		return nil, err
	}

	if _, err := r.ReadAt(b[:4], 0); err != nil {
		return nil, fmt.Errorf("reading magic header of parquet file: %w", err)
	}
	if string(b[:4]) != "PAR1" {
		return nil, fmt.Errorf("invalid magic header of parquet file: %q", b[:4])
	}

	if _, err := r.ReadAt(b[:8], size-8); err != nil {
		return nil, fmt.Errorf("reading magic footer of parquet file: %w", err)
	}
	if string(b[4:8]) != "PAR1" {
		return nil, fmt.Errorf("invalid magic footer of parquet file: %q", b[:4])
	}

	footerSize := int64(binary.LittleEndian.Uint32(b[:4]))
	section := acquireBufferedSectionReader(r, size-(footerSize+8), footerSize)
	decoder := thrift.NewDecoder(f.protocol.NewReader(section))
	defer releaseBufferedSectionReader(section)

	err := decoder.Decode(&f.metadata)
	if err != nil {
		return nil, fmt.Errorf("reading parquet file metadata: %w", err)
	}
	if len(f.metadata.Schema) == 0 {
		return nil, ErrMissingRootColumn
	}

	if !c.SkipPageIndex {
		if f.columnIndexes, f.offsetIndexes, err = f.readPageIndex(section, decoder); err != nil {
			return nil, fmt.Errorf("reading page index of parquet file: %w", err)
		}
	}

	if f.root, err = openColumns(f); err != nil {
		return nil, fmt.Errorf("opening columns of parquet file: %w", err)
	}
	return f, nil
}

// ReadPageIndex reads the page index section of the parquet file f.
//
// If the file did not contain a page index, the method returns two empty slices
// and a nil error.
//
// This method is useful in combination with the SkipPageIndex option to delay
// reading the page index section until after the file was opened. Note that in
// this case the page index is not cached within the file, programs are expected
// to make use of independently from the parquet package.
func (f *File) ReadPageIndex() ([]ColumnIndex, []OffsetIndex, error) {
	section := acquireBufferedSectionReader(nil, 0, 0)
	decoder := thrift.NewDecoder(f.protocol.NewReader(section))
	defer releaseBufferedSectionReader(section)
	return f.readPageIndex(section, decoder)
}

func (f *File) readPageIndex(section *bufferedSectionReader, decoder *thrift.Decoder) ([]ColumnIndex, []OffsetIndex, error) {
	if len(f.metadata.RowGroups) == 0 || len(f.metadata.RowGroups[0].Columns) == 0 {
		return nil, nil, nil
	}

	indexOffset := f.metadata.RowGroups[0].Columns[0].ColumnIndexOffset
	indexLength := int64(0)

	if indexOffset == 0 {
		// A zero offset means that the file does not contain a column index.
		return nil, nil, nil
	}

	for i := range f.metadata.RowGroups {
		for j := range f.metadata.RowGroups[i].Columns {
			indexLength += int64(f.metadata.RowGroups[i].Columns[j].ColumnIndexLength)
			indexLength += int64(f.metadata.RowGroups[i].Columns[j].OffsetIndexLength)
		}
	}

	numColumnChunks := len(f.metadata.RowGroups) * len(f.metadata.RowGroups[0].Columns)
	columnIndexes := make([]ColumnIndex, 0, numColumnChunks)
	offsetIndexes := make([]OffsetIndex, 0, numColumnChunks)
	section.reset(f.reader, indexOffset, indexLength)

	for i := range f.metadata.RowGroups {
		for range f.metadata.RowGroups[i].Columns {
			n := len(columnIndexes)
			columnIndexes = append(columnIndexes, ColumnIndex{})

			if err := decoder.Decode(&columnIndexes[n]); err != nil {
				return nil, nil, err
			}
		}
	}

	for i := range f.metadata.RowGroups {
		for range f.metadata.RowGroups[i].Columns {
			n := len(offsetIndexes)
			offsetIndexes = append(offsetIndexes, OffsetIndex{})

			if err := decoder.Decode(&offsetIndexes[n]); err != nil {
				return nil, nil, err
			}
		}
	}

	return columnIndexes, offsetIndexes, nil
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

// ColumnIndexes returns the page index of the parquet file f.
//
// If the file did not contain a column index, the method returns an empty slice
// and nil error.
func (f *File) ColumnIndexes() []ColumnIndex { return f.columnIndexes }

// OffsetIndexes returns the page index of the parquet file f.
//
// If the file did not contain an offset index, the method returns an empty
// slice and nil error.
func (f *File) OffsetIndexes() []OffsetIndex { return f.offsetIndexes }

func (f *File) readColumnIndex(chunk *format.ColumnChunk) (*ColumnIndex, error) {
	columnIndex := new(format.ColumnIndex)

	section := acquireBufferedSectionReader(f.reader, chunk.ColumnIndexOffset, int64(chunk.ColumnIndexLength))
	defer releaseBufferedSectionReader(section)

	err := thrift.NewDecoder(f.protocol.NewReader(section)).Decode(columnIndex)
	return (*ColumnIndex)(columnIndex), err
}

func (f *File) readOffsetIndex(chunk *format.ColumnChunk) (*OffsetIndex, error) {
	offsetIndex := new(format.OffsetIndex)

	section := acquireBufferedSectionReader(f.reader, chunk.OffsetIndexOffset, int64(chunk.OffsetIndexLength))
	defer releaseBufferedSectionReader(section)

	err := thrift.NewDecoder(f.protocol.NewReader(section)).Decode(offsetIndex)
	return (*OffsetIndex)(offsetIndex), err
}

var (
	_ io.ReaderAt = (*File)(nil)

	bufferedSectionReaderPool sync.Pool
)

type bufferedSectionReader struct {
	section io.SectionReader
	bufio.Reader
}

func newBufferedSectionReader(r io.ReaderAt, offset, length int64) *bufferedSectionReader {
	b := &bufferedSectionReader{section: *io.NewSectionReader(r, offset, length)}
	b.Reader = *bufio.NewReaderSize(&b.section, defaultBufferSize)
	return b
}

func (b *bufferedSectionReader) reset(r io.ReaderAt, offset, length int64) {
	b.section = *io.NewSectionReader(r, offset, length)
	b.Reader.Reset(&b.section)
}

func acquireBufferedSectionReader(r io.ReaderAt, offset, length int64) *bufferedSectionReader {
	b, _ := bufferedSectionReaderPool.Get().(*bufferedSectionReader)
	if b == nil {
		b = newBufferedSectionReader(r, offset, length)
	} else {
		b.reset(r, offset, length)
	}
	return b
}

func releaseBufferedSectionReader(b *bufferedSectionReader) {
	b.reset(nil, 0, 0)
	bufferedSectionReaderPool.Put(b)
}
