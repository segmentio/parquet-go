package parquet

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet-go/format"
)

const (
	defaultDictBufferSize  = 8192
	defaultReadBufferSize  = 4096
	defaultLevelBufferSize = 1024
)

// File represents a parquet file. The layout of a Parquet file can be found
// here: https://github.com/apache/parquet-format#file-format
type File struct {
	metadata            format.FileMetaData
	protocol            thrift.CompactProtocol
	reader              io.ReaderAt
	size                int64
	schema              *Schema
	root                *Column
	columnIndexes       []format.ColumnIndex
	offsetIndexes       []format.OffsetIndex
	rowGroups           []RowGroup
	GetIOReaderFromPath func(filepath string) io.ReaderAt
}

// OpenFile opens a parquet file and reads the content between offset 0 and the given
// size in r.
//
// Only the parquet magic bytes and footer are read, column chunks and other
// parts of the file are left untouched; this means that successfully opening
// a file does not validate that the pages have valid checksums.
func OpenFile(r io.ReaderAt, size int64, options ...FileOption) (*File, error) {
	b := make([]byte, 8)
	f := &File{reader: r, size: size}
	c, err := NewFileConfig(options...)
	if err != nil {
		return nil, err
	}

	// used to get secondary io.ReaderAt interfaces for columnChunk page reads
	if c.GetIOReaderFromPath != nil {
		f.GetIOReaderFromPath = c.GetIOReaderFromPath
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
		return nil, fmt.Errorf("invalid magic footer of parquet file: %q", b[4:8])
	}

	footerSize := int64(binary.LittleEndian.Uint32(b[:4]))
	footerData := make([]byte, footerSize)

	if _, err := f.reader.ReadAt(footerData, size-(footerSize+8)); err != nil {
		return nil, fmt.Errorf("reading footer of parquet file: %w", err)
	}
	if err := thrift.Unmarshal(&f.protocol, footerData, &f.metadata); err != nil {
		return nil, fmt.Errorf("reading parquet file metadata: %w", err)
	}
	if len(f.metadata.Schema) == 0 {
		return nil, ErrMissingRootColumn
	}

	if !c.SkipPageIndex {
		if f.columnIndexes, f.offsetIndexes, err = f.ReadPageIndex(); err != nil {
			return nil, fmt.Errorf("reading page index of parquet file: %w", err)
		}
	}

	if f.root, err = openColumns(f); err != nil {
		return nil, fmt.Errorf("opening columns of parquet file: %w", err)
	}

	schema := NewSchema(f.root.Name(), f.root)
	columns := make([]*Column, 0, MaxColumnIndex+1)
	f.schema = schema
	f.root.forEachLeaf(func(c *Column) { columns = append(columns, c) })

	rowGroups := make([]fileRowGroup, len(f.metadata.RowGroups))
	for i := range rowGroups {
		rowGroups[i].init(f, schema, columns, &f.metadata.RowGroups[i])
	}
	f.rowGroups = make([]RowGroup, len(rowGroups))
	for i := range rowGroups {
		f.rowGroups[i] = &rowGroups[i]
	}

	if !c.SkipBloomFilters {
		h := format.BloomFilterHeader{}
		p := thrift.CompactProtocol{}
		s := io.NewSectionReader(r, 0, size)
		d := thrift.NewDecoder(p.NewReader(s))

		for i := range rowGroups {
			g := &rowGroups[i]

			for j := range g.columns {
				c := g.columns[j].(*fileColumnChunk)

				if offset := c.chunk.MetaData.BloomFilterOffset; offset > 0 {
					s.Seek(offset, io.SeekStart)
					h = format.BloomFilterHeader{}
					if err := d.Decode(&h); err != nil {
						return nil, err
					}
					offset, _ = s.Seek(0, io.SeekCurrent)
					c.bloomFilter = newBloomFilter(r, offset, &h)
				}
			}
		}
	}

	sortKeyValueMetadata(f.metadata.KeyValueMetadata)
	return f, nil
}

// ReadPageIndex reads the page index section of the parquet file f.
//
// If the file did not contain a page index, the method returns two empty slices
// and a nil error.
//
// Only leaf columns have indexes, the returned indexes are arranged using the
// following layout:
//
//	+ -------------- +
//	| col 0: chunk 0 |
//	+ -------------- +
//	| col 1: chunk 0 |
//	+ -------------- +
//	| ...            |
//	+ -------------- +
//	| col 0: chunk 1 |
//	+ -------------- +
//	| col 1: chunk 1 |
//	+ -------------- +
//	| ...            |
//	+ -------------- +
//
// This method is useful in combination with the SkipPageIndex option to delay
// reading the page index section until after the file was opened. Note that in
// this case the page index is not cached within the file, programs are expected
// to make use of independently from the parquet package.
func (f *File) ReadPageIndex() ([]format.ColumnIndex, []format.OffsetIndex, error) {
	columnIndexOffset := f.metadata.RowGroups[0].Columns[0].ColumnIndexOffset
	offsetIndexOffset := f.metadata.RowGroups[0].Columns[0].OffsetIndexOffset
	columnIndexLength := int64(0)
	offsetIndexLength := int64(0)

	if columnIndexOffset == 0 || offsetIndexOffset == 0 {
		return nil, nil, nil
	}

	forEachColumnChunk := func(do func(int, int, *format.ColumnChunk) error) error {
		for i := range f.metadata.RowGroups {
			for j := range f.metadata.RowGroups[i].Columns {
				c := &f.metadata.RowGroups[i].Columns[j]
				if err := do(i, j, c); err != nil {
					return err
				}
			}
		}
		return nil
	}

	forEachColumnChunk(func(_, _ int, c *format.ColumnChunk) error {
		columnIndexLength += int64(c.ColumnIndexLength)
		offsetIndexLength += int64(c.OffsetIndexLength)
		return nil
	})

	numRowGroups := len(f.metadata.RowGroups)
	numColumns := len(f.metadata.RowGroups[0].Columns)
	numColumnChunks := numRowGroups * numColumns

	columnIndexes := make([]format.ColumnIndex, numColumnChunks)
	offsetIndexes := make([]format.OffsetIndex, numColumnChunks)
	indexBuffer := make([]byte, max(int(columnIndexLength), int(offsetIndexLength)))

	if columnIndexOffset > 0 {
		columnIndexData := indexBuffer[:columnIndexLength]

		if _, err := f.reader.ReadAt(columnIndexData, columnIndexOffset); err != nil {
			return nil, nil, fmt.Errorf("reading %d bytes column index at offset %d: %w", columnIndexLength, columnIndexOffset, err)
		}

		err := forEachColumnChunk(func(i, j int, c *format.ColumnChunk) error {
			offset := c.ColumnIndexOffset - columnIndexOffset
			length := int64(c.ColumnIndexLength)
			buffer := columnIndexData[offset : offset+length]
			if err := thrift.Unmarshal(&f.protocol, buffer, &columnIndexes[(i*numColumns)+j]); err != nil {
				return fmt.Errorf("decoding column index: rowGroup=%d columnChunk=%d/%d: %w", i, j, numColumns, err)
			}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}

	if offsetIndexOffset > 0 {
		offsetIndexData := indexBuffer[:offsetIndexLength]

		if _, err := f.reader.ReadAt(offsetIndexData, offsetIndexOffset); err != nil {
			return nil, nil, fmt.Errorf("reading %d bytes offset index at offset %d: %w", offsetIndexLength, offsetIndexOffset, err)
		}

		err := forEachColumnChunk(func(i, j int, c *format.ColumnChunk) error {
			offset := c.OffsetIndexOffset - offsetIndexOffset
			length := int64(c.OffsetIndexLength)
			buffer := offsetIndexData[offset : offset+length]
			if err := thrift.Unmarshal(&f.protocol, buffer, &offsetIndexes[(i*numColumns)+j]); err != nil {
				return fmt.Errorf("decoding column index: rowGroup=%d columnChunk=%d/%d: %w", i, j, numColumns, err)
			}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}

	return columnIndexes, offsetIndexes, nil
}

// NumRows returns the number of rows in the file.
func (f *File) NumRows() int64 { return f.metadata.NumRows }

// RowGroups returns the list of row group in the file.
func (f *File) RowGroups() []RowGroup { return f.rowGroups }

// Root returns the root column of f.
func (f *File) Root() *Column { return f.root }

// Schema returns the schema of f.
func (f *File) Schema() *Schema { return f.schema }

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
func (f *File) ColumnIndexes() []format.ColumnIndex { return f.columnIndexes }

// OffsetIndexes returns the page index of the parquet file f.
//
// If the file did not contain an offset index, the method returns an empty
// slice and nil error.
func (f *File) OffsetIndexes() []format.OffsetIndex { return f.offsetIndexes }

// Lookup returns the value associated with the given key in the file key/value
// metadata.
//
// The ok boolean will be true if the key was found, false otherwise.
func (f *File) Lookup(key string) (value string, ok bool) {
	return lookupKeyValueMetadata(f.metadata.KeyValueMetadata, key)
}

func (f *File) hasIndexes() bool {
	return f.columnIndexes != nil && f.offsetIndexes != nil
}

var (
	_ io.ReaderAt = (*File)(nil)
)

func sortKeyValueMetadata(keyValueMetadata []format.KeyValue) {
	sort.Slice(keyValueMetadata, func(i, j int) bool {
		switch {
		case keyValueMetadata[i].Key < keyValueMetadata[j].Key:
			return true
		case keyValueMetadata[i].Key > keyValueMetadata[j].Key:
			return false
		default:
			return keyValueMetadata[i].Value < keyValueMetadata[j].Value
		}
	})
}

func lookupKeyValueMetadata(keyValueMetadata []format.KeyValue, key string) (value string, ok bool) {
	i := sort.Search(len(keyValueMetadata), func(i int) bool {
		return keyValueMetadata[i].Key >= key
	})
	if i == len(keyValueMetadata) || keyValueMetadata[i].Key != key {
		return "", false
	}
	return keyValueMetadata[i].Value, true
}

type fileRowGroup struct {
	schema   *Schema
	rowGroup *format.RowGroup
	columns  []ColumnChunk
	sorting  []SortingColumn
}

func (g *fileRowGroup) init(file *File, schema *Schema, columns []*Column, rowGroup *format.RowGroup) {
	g.schema = schema
	g.rowGroup = rowGroup
	g.columns = make([]ColumnChunk, len(rowGroup.Columns))
	g.sorting = make([]SortingColumn, len(rowGroup.SortingColumns))
	fileColumnChunks := make([]fileColumnChunk, len(rowGroup.Columns))

	for i := range g.columns {
		fileColumnChunks[i] = fileColumnChunk{
			file:     file,
			column:   columns[i],
			rowGroup: rowGroup,
			chunk:    &rowGroup.Columns[i],
		}

		if file.hasIndexes() {
			j := (int(rowGroup.Ordinal) * len(columns)) + i
			fileColumnChunks[i].columnIndex = &file.columnIndexes[j]
			fileColumnChunks[i].offsetIndex = &file.offsetIndexes[j]
		}

		g.columns[i] = &fileColumnChunks[i]
	}

	for i := range g.sorting {
		g.sorting[i] = &fileSortingColumn{
			column:     columns[rowGroup.SortingColumns[i].ColumnIdx],
			descending: rowGroup.SortingColumns[i].Descending,
			nullsFirst: rowGroup.SortingColumns[i].NullsFirst,
		}
	}
}

func (g *fileRowGroup) Schema() *Schema                 { return g.schema }
func (g *fileRowGroup) NumRows() int64                  { return g.rowGroup.NumRows }
func (g *fileRowGroup) ColumnChunks() []ColumnChunk     { return g.columns }
func (g *fileRowGroup) SortingColumns() []SortingColumn { return g.sorting }
func (g *fileRowGroup) Rows() Rows                      { return &rowGroupRowReader{rowGroup: g} }

type fileSortingColumn struct {
	column     *Column
	descending bool
	nullsFirst bool
}

func (s *fileSortingColumn) Path() []string   { return s.column.Path() }
func (s *fileSortingColumn) Descending() bool { return s.descending }
func (s *fileSortingColumn) NullsFirst() bool { return s.nullsFirst }

type fileColumnChunk struct {
	file        *File
	column      *Column
	bloomFilter *bloomFilter
	rowGroup    *format.RowGroup
	columnIndex *format.ColumnIndex
	offsetIndex *format.OffsetIndex
	chunk       *format.ColumnChunk
}

func (c *fileColumnChunk) Type() Type {
	return c.column.Type()
}

func (c *fileColumnChunk) Column() int {
	return int(c.column.Index())
}

func (c *fileColumnChunk) Pages() Pages {
	r := new(filePages)
	r.init(c)
	return r
}

func (c *fileColumnChunk) ColumnIndex() ColumnIndex {
	if c.columnIndex == nil {
		return nil
	}
	return fileColumnIndex{c}
}

func (c *fileColumnChunk) OffsetIndex() OffsetIndex {
	if c.offsetIndex == nil {
		return nil
	}
	return (*fileOffsetIndex)(c.offsetIndex)
}

func (c *fileColumnChunk) BloomFilter() BloomFilter {
	if c.bloomFilter == nil {
		return nil
	}
	return c.bloomFilter
}

func (c *fileColumnChunk) NumValues() int64 {
	return c.chunk.MetaData.NumValues
}

type filePages struct {
	chunk    *fileColumnChunk
	dictPage *dictPage
	dataPage *dataPage
	section  *io.SectionReader
	rbuf     *bufio.Reader

	protocol thrift.CompactProtocol
	decoder  thrift.Decoder

	baseOffset int64
	dataOffset int64
	dictOffset int64
	index      int
	skip       int64
}

func (r *filePages) init(c *fileColumnChunk) {
	r.dataPage = new(dataPage)
	r.chunk = c
	r.baseOffset = c.chunk.MetaData.DataPageOffset
	r.dataOffset = r.baseOffset
	if c.chunk.MetaData.DictionaryPageOffset != 0 {
		r.baseOffset = c.chunk.MetaData.DictionaryPageOffset
		r.dictOffset = r.baseOffset
	}
	r.section = io.NewSectionReader(c.file, r.baseOffset, c.chunk.MetaData.TotalCompressedSize)
	r.rbuf = bufio.NewReaderSize(r.section, defaultReadBufferSize)
	r.decoder.Reset(r.protocol.NewReader(r.rbuf))
}

func (r *filePages) ReadPage() (Page, error) {
	for {
		header := new(format.PageHeader)
		if err := r.decoder.Decode(header); err != nil {
			return nil, err
		}

		if cap(r.dataPage.data) < int(header.CompressedPageSize) {
			r.dataPage.data = make([]byte, header.CompressedPageSize)
		} else {
			r.dataPage.data = r.dataPage.data[:header.CompressedPageSize]
		}

		if cap(r.dataPage.values) < int(header.UncompressedPageSize) {
			r.dataPage.values = make([]byte, 0, header.UncompressedPageSize)
		}

		if _, err := io.ReadFull(r.rbuf, r.dataPage.data); err != nil {
			return nil, err
		}

		if header.CRC != 0 {
			headerChecksum := uint32(header.CRC)
			bufferChecksum := crc32.ChecksumIEEE(r.dataPage.data)

			if headerChecksum != bufferChecksum {
				// The parquet specs indicate that corruption errors could be
				// handled gracefully by skipping pages, tho this may not always
				// be practical. Depending on how the pages are consumed,
				// missing rows may cause unpredictable behaviors in algorithms.
				//
				// For now, we assume these errors to be fatal, but we may
				// revisit later and improve error handling to be more resilient
				// to data corruption.
				return nil, fmt.Errorf("crc32 checksum mismatch in page %d of column %q: 0x%08X != 0x%08X: %w",
					r.index,
					r.columnPath(),
					headerChecksum,
					bufferChecksum,
					ErrCorrupted,
				)
			}
		}

		var column = r.chunk.column
		var page Page
		var err error

		switch header.Type {
		case format.DataPageV2:
			if header.DataPageHeaderV2 == nil {
				err = ErrMissingPageHeader
			} else {
				page, err = column.decodeDataPageV2(DataPageHeaderV2{header.DataPageHeaderV2}, r.dataPage)
			}

		case format.DataPage:
			if header.DataPageHeader == nil {
				err = ErrMissingPageHeader
			} else {
				page, err = column.decodeDataPageV1(DataPageHeaderV1{header.DataPageHeader}, r.dataPage)
			}

		case format.DictionaryPage:
			// Sometimes parquet files do not have the dictionary page offset
			// recorded in the column metadata. We account for this by lazily
			// checking whether the first page is a dictionary page.
			if header.DictionaryPageHeader == nil {
				err = ErrMissingPageHeader
			} else if r.index > 0 {
				err = ErrUnexpectedDictionaryPage
			} else {
				r.dictPage = new(dictPage)
				r.dataPage.dictionary, err = column.decodeDictionary(
					DictionaryPageHeader{header.DictionaryPageHeader},
					r.dataPage,
					r.dictPage,
				)
			}

		default:
			err = fmt.Errorf("cannot read values of type %s from page", header.Type)
		}

		if err != nil {
			return nil, fmt.Errorf("decoding page %d of column %q: %w", r.index, r.columnPath(), err)
		}

		if page != nil {
			r.index++
			if r.skip == 0 {
				return page, nil
			}

			// TODO: what about pages that don't embed the number of rows?
			// (data page v1 with no offset index in the column chunk).
			numRows := page.NumRows()
			if numRows > r.skip {
				seek := r.skip
				r.skip = 0
				if seek > 0 {
					page = page.Buffer().Slice(seek, numRows)
				}
				return page, nil
			}

			r.skip -= numRows
		}
	}
}

func (r *filePages) columnPath() columnPath {
	return columnPath(r.chunk.column.Path())
}

func (r *filePages) SeekToRow(rowIndex int64) (err error) {
	if r.chunk.offsetIndex == nil {
		_, err = r.section.Seek(r.dataOffset-r.baseOffset, io.SeekStart)
		r.skip = rowIndex
		r.index = 0
		if r.dictOffset > 0 {
			r.index = 1
		}
	} else {
		pages := r.chunk.offsetIndex.PageLocations
		index := sort.Search(len(pages), func(i int) bool {
			return pages[i].FirstRowIndex > rowIndex
		}) - 1
		if index < 0 {
			return ErrSeekOutOfRange
		}
		_, err = r.section.Seek(pages[index].Offset-r.baseOffset, io.SeekStart)
		r.skip = rowIndex - pages[index].FirstRowIndex
		r.index = index
	}
	r.rbuf.Reset(r.section)
	return err
}
