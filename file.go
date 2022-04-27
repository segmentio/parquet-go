package parquet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet-go/encoding"
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
	metadata      format.FileMetaData
	protocol      thrift.CompactProtocol
	reader        io.ReaderAt
	size          int64
	schema        *Schema
	root          *Column
	columnIndexes []format.ColumnIndex
	offsetIndexes []format.OffsetIndex
	rowGroups     []RowGroup
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
func (g *fileRowGroup) Rows() Rows                      { return &rowGroupRows{rowGroup: g} }

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
	c.setPagesOn(r)
	return r
}

func (c *fileColumnChunk) setPagesOn(r *filePages) {
	r.column = c
	r.page = filePage{
		column:     c.column,
		columnType: c.column.Type(),
		codec:      c.chunk.MetaData.Codec,
	}
	r.baseOffset = c.chunk.MetaData.DataPageOffset
	r.dataOffset = r.baseOffset
	if c.chunk.MetaData.DictionaryPageOffset != 0 {
		r.baseOffset = c.chunk.MetaData.DictionaryPageOffset
		r.dictOffset = r.baseOffset
	}
	r.section = io.NewSectionReader(c.file, r.baseOffset, c.chunk.MetaData.TotalCompressedSize)
	r.rbuf = bufio.NewReaderSize(r.section, defaultReadBufferSize)
	r.section.Seek(r.dataOffset-r.baseOffset, io.SeekStart)
	r.decoder.Reset(r.protocol.NewReader(r.rbuf))
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
	column     *fileColumnChunk
	protocol   thrift.CompactProtocol
	decoder    thrift.Decoder
	baseOffset int64
	dictOffset int64
	dataOffset int64

	section *io.SectionReader
	rbuf    *bufio.Reader
	limit   io.LimitedReader

	// This buffer holds compressed pages in memory when they are read; we need
	// to read whole pages because we have to compute the checksum prior to
	// exposing the page to the application.
	compressedPageData *bytes.Buffer

	page filePage
	skip int64
}

func (r *filePages) Close() error {
	if r.page.values != nil {
		r.page.values.release()
		r.page.values = nil
	}

	releaseCompressedPageBuffer(r.compressedPageData)
	r.compressedPageData = nil

	r.rbuf = nil
	return nil
}

func (r *filePages) readPage() (*filePage, error) {
	if r.rbuf == nil {
		return nil, io.EOF
	}

	r.page.header = format.PageHeader{}

	/*
		h := &r.page.header
			h.Type = 0
			h.UncompressedPageSize = 0
			h.CompressedPageSize = 0
			h.CRC = 0

			if h.DataPageHeader != nil {
				*h.DataPageHeader = format.DataPageHeader{}
			}
			if h.IndexPageHeader != nil {
				h.IndexPageHeader = nil
			}
			if h.DictionaryPageHeader != nil {
				h.DictionaryPageHeader = nil
			}
			if h.DataPageHeaderV2 != nil {
				*h.DataPageHeaderV2 = format.DataPageHeaderV2{}
			}
	*/

	if err := r.decoder.Decode(&r.page.header); err != nil {
		if err != io.EOF {
			err = fmt.Errorf("decoding page header: %w", err)
		}
		return nil, err
	}

	if r.compressedPageData == nil {
		r.compressedPageData = acquireCompressedPageBuffer()
	}
	r.compressedPageData.Reset()
	r.compressedPageData.Grow(int(r.page.header.CompressedPageSize))

	r.limit.R = r.rbuf
	r.limit.N = int64(r.page.header.CompressedPageSize)

	_, err := r.compressedPageData.ReadFrom(&r.limit)
	if err != nil {
		return nil, fmt.Errorf("reading page %d of column %q", r.page.index, r.page.columnPath())
	}

	if r.page.header.CRC != 0 {
		headerChecksum := uint32(r.page.header.CRC)
		bufferChecksum := crc32.ChecksumIEEE(r.compressedPageData.Bytes())

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
				r.page.index,
				r.page.columnPath(),
				headerChecksum,
				bufferChecksum,
				ErrCorrupted,
			)
		}
	}

	r.page.data.Reset(r.compressedPageData.Bytes())

	if r.column.columnIndex != nil {
		err = r.page.parseColumnIndex(r.column.columnIndex)
	} else {
		err = r.page.parseStatistics()
	}
	return &r.page, err
}

func (r *filePages) readDictionary() error {
	if _, err := r.section.Seek(r.dictOffset-r.baseOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seeking to dictionary page offset: %w", err)
	}
	r.rbuf.Reset(r.section)
	p, err := r.readPage()
	if err != nil {
		return err
	}
	return r.readDictionaryPage(p)
}

func (r *filePages) readDictionaryPage(p *filePage) error {
	page := acquireCompressedPageReader(p.codec, &p.data)
	enc := p.header.DictionaryPageHeader.Encoding
	dec := LookupEncoding(enc).NewDecoder(page)

	columnIndex := r.column.Column()
	numValues := int(p.NumValues())
	dict, err := p.columnType.ReadDictionary(columnIndex, numValues, dec)
	releaseCompressedPageReader(page)

	if err != nil {
		return fmt.Errorf("reading dictionary of column %q: %w", p.columnPath(), err)
	}
	r.page.dictionary = dict
	r.page.columnType = dict.Type()
	return nil
}

func (r *filePages) ReadPage() (Page, error) {
	if r.page.dictionary == nil && r.dictOffset > 0 {
		if err := r.readDictionary(); err != nil {
			return nil, err
		}
	}

	for {
		p, err := r.readPage()
		if err != nil {
			return nil, err
		}

		// Sometimes parquet files do not have the dictionary page offset
		// recorded in the column metadata. We account for this by lazily
		// checking whether the first page is a dictionary page.
		if p.index == 0 && p.header.Type == format.DictionaryPage && r.page.dictionary == nil {
			offset, err := r.section.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
			r.dictOffset = r.baseOffset
			r.dataOffset = r.baseOffset + offset
			if err := r.readDictionaryPage(p); err != nil {
				return nil, err
			}
			continue
		}

		p.index++
		if r.skip == 0 {
			return p, nil
		}

		numRows := p.NumRows()
		if numRows > r.skip {
			seek := r.skip
			r.skip = 0
			if seek > 0 {
				return p.Buffer().Slice(seek, numRows), nil
			}
			return p, nil
		}

		r.skip -= numRows
	}
}

func (r *filePages) SeekToRow(rowIndex int64) (err error) {
	if r.column.offsetIndex == nil {
		_, err = r.section.Seek(r.dataOffset-r.baseOffset, io.SeekStart)
		r.skip = rowIndex
		r.page.index = 0
	} else {
		pages := r.column.offsetIndex.PageLocations
		index := sort.Search(len(pages), func(i int) bool {
			return pages[i].FirstRowIndex > rowIndex
		}) - 1
		if index < 0 {
			return ErrSeekOutOfRange
		}
		_, err = r.section.Seek(pages[index].Offset-r.baseOffset, io.SeekStart)
		r.skip = rowIndex - pages[index].FirstRowIndex
		r.page.index = index
	}
	r.rbuf.Reset(r.section)
	return err
}

type filePage struct {
	column     *Column
	columnType Type
	dictionary Dictionary

	codec  format.CompressionCodec
	header format.PageHeader
	data   bytes.Reader

	index     int
	minValue  Value
	maxValue  Value
	hasBounds bool

	// This field caches the state used when reading values from the page.
	// We allocate it separately to avoid creating it if the Values method
	// is never called (e.g. when the page data is never decompressed).
	// The state is released when the parent page reader reaches EOF.
	values *filePageValueReaderState
}

var (
	errPageIndexExceedsColumnIndexNullPages  = errors.New("page index exceeds column index null pages")
	errPageIndexExceedsColumnIndexMinValues  = errors.New("page index exceeds column index min values")
	errPageIndexExceedsColumnIndexMaxValues  = errors.New("page index exceeds column index max values")
	errPageIndexExceedsColumnIndexNullCounts = errors.New("page index exceeds column index null counts")
)

func (p *filePage) statistics() *format.Statistics {
	switch p.header.Type {
	case format.DataPageV2:
		return &p.header.DataPageHeaderV2.Statistics
	case format.DataPage:
		return &p.header.DataPageHeader.Statistics
	default:
		return nil
	}
}

func (p *filePage) parseColumnIndex(columnIndex *format.ColumnIndex) (err error) {
	if p.index >= len(columnIndex.NullPages) {
		return p.errColumnIndex(errPageIndexExceedsColumnIndexNullPages)
	}
	if p.index >= len(columnIndex.MinValues) {
		return p.errColumnIndex(errPageIndexExceedsColumnIndexMinValues)
	}
	if p.index >= len(columnIndex.MaxValues) {
		return p.errColumnIndex(errPageIndexExceedsColumnIndexMaxValues)
	}
	if p.index >= len(columnIndex.NullCounts) {
		return p.errColumnIndex(errPageIndexExceedsColumnIndexNullCounts)
	}

	minValue := columnIndex.MinValues[p.index]
	maxValue := columnIndex.MaxValues[p.index]

	if stats := p.statistics(); stats != nil {
		if stats.MinValue == nil {
			stats.MinValue = minValue
		}
		if stats.MaxValue == nil {
			stats.MaxValue = maxValue
		}
		if stats.NullCount == 0 {
			stats.NullCount = columnIndex.NullCounts[p.index]
		}
	}

	if columnIndex.NullPages[p.index] {
		p.minValue = Value{}
		p.maxValue = Value{}
		p.hasBounds = false
	} else {
		kind := p.columnType.Kind()
		p.minValue, err = parseValue(kind, minValue)
		if err != nil {
			return p.errColumnIndex(err)
		}
		p.maxValue, err = parseValue(kind, maxValue)
		if err != nil {
			return p.errColumnIndex(err)
		}
		p.hasBounds = true
	}

	return nil
}

func (p *filePage) parseStatistics() (err error) {
	kind := p.columnType.Kind()
	stats := p.statistics()

	if stats == nil {
		// The column has no index and page has no statistics,
		// default to reporting that the min and max are both null.
		p.minValue = Value{}
		p.maxValue = Value{}
		p.hasBounds = false
		return nil
	}

	if stats.MinValue == nil {
		p.minValue = Value{}
	} else {
		p.minValue, err = parseValue(kind, stats.MinValue)
		if err != nil {
			return p.errStatistics(err)
		}
	}

	if stats.MaxValue == nil {
		p.maxValue = Value{}
	} else {
		p.maxValue, err = parseValue(kind, stats.MaxValue)
		if err != nil {
			return p.errStatistics(err)
		}
	}

	p.hasBounds = true
	return nil
}

func (p *filePage) errColumnIndex(err error) error {
	return fmt.Errorf("reading bounds of page %d from index of column %q: %w", p.index, p.columnPath(), err)
}

func (p *filePage) errStatistics(err error) error {
	return fmt.Errorf("reading bounds of page %d from statistics in column %q: %w", p.index, p.columnPath(), err)
}

func (p *filePage) columnPath() columnPath {
	return columnPath(p.column.Path())
}

func (p *filePage) Column() int {
	return int(p.column.Index())
}

func (p *filePage) Dictionary() Dictionary {
	return p.dictionary
}

func (p *filePage) NumRows() int64 {
	switch p.header.Type {
	case format.DataPageV2:
		return int64(p.header.DataPageHeaderV2.NumRows)
	default:
		return 0
	}
}

func (p *filePage) NumValues() int64 {
	switch p.header.Type {
	case format.DataPageV2:
		return int64(p.header.DataPageHeaderV2.NumValues)
	case format.DataPage:
		return int64(p.header.DataPageHeader.NumValues)
	case format.DictionaryPage:
		return int64(p.header.DictionaryPageHeader.NumValues)
	default:
		return 0
	}
}

func (p *filePage) NumNulls() int64 {
	switch p.header.Type {
	case format.DataPageV2:
		return int64(p.header.DataPageHeaderV2.NumNulls)
	case format.DataPage:
		return p.header.DataPageHeader.Statistics.NullCount
	default:
		return 0
	}
}

func (p *filePage) Bounds() (min, max Value, ok bool) {
	return p.minValue, p.maxValue, p.hasBounds
}

func (p *filePage) Size() int64 {
	return int64(p.header.UncompressedPageSize)
}

func (p *filePage) Values() ValueReader {
	if p.values == nil {
		p.values = new(filePageValueReaderState)
	}
	if err := p.values.init(p.columnType, p.column, p.codec, p.PageHeader(), &p.data, p.dictionary); err != nil {
		return &errorValueReader{err: err}
	}
	return p.values.reader
}

func (p *filePage) Buffer() BufferedPage {
	bufferedPage := p.column.Type().NewColumnBuffer(p.Column(), int(p.Size()))
	_, err := CopyValues(bufferedPage, p.Values())
	if err != nil {
		return &errorPage{err: err, columnIndex: p.Column()}
	}
	return bufferedPage.Page()
}

func (p *filePage) PageHeader() PageHeader {
	switch p.header.Type {
	case format.DataPageV2:
		return DataPageHeaderV2{p.header.DataPageHeaderV2}
	case format.DataPage:
		return DataPageHeaderV1{p.header.DataPageHeader}
	case format.DictionaryPage:
		return DictionaryPageHeader{p.header.DictionaryPageHeader}
	default:
		return unknownPageHeader{&p.header}
	}
}

func (p *filePage) PageData() io.Reader { return &p.data }

func (p *filePage) PageSize() int64 { return int64(p.header.CompressedPageSize) }

func (p *filePage) CRC() uint32 { return uint32(p.header.CRC) }

type filePageValueReaderState struct {
	reader ColumnReader

	v1 struct {
		repetitions dataPageLevelV1
		definitions dataPageLevelV1
	}

	v2 struct {
		repetitions dataPageLevelV2
		definitions dataPageLevelV2
	}

	repetitions struct {
		encoding format.Encoding
		decoder  encoding.Decoder
	}

	definitions struct {
		encoding format.Encoding
		decoder  encoding.Decoder
	}

	page struct {
		encoding   format.Encoding
		decoder    encoding.Decoder
		compressed *compressedPageReader
	}
}

func (s *filePageValueReaderState) release() {
	if s.page.compressed != nil {
		releaseCompressedPageReader(s.page.compressed)
		s.page.compressed = nil
	}
}

func (s *filePageValueReaderState) init(columnType Type, column *Column, codec format.CompressionCodec, header PageHeader, data *bytes.Reader, dict Dictionary) (err error) {
	var repetitionLevels io.Reader
	var definitionLevels io.Reader
	var pageHeader DataPageHeader
	var pageData io.Reader

	switch h := header.(type) {
	case DataPageHeaderV2:
		repetitionLevels, definitionLevels, err = s.initDataPageV2(h, data)
		if err != nil {
			return fmt.Errorf("initializing v2 reader for page of column %q: %w", columnPath(column.Path()), err)
		}
		if h.IsCompressed(codec) {
			s.page.compressed = makeCompressedPage(s.page.compressed, codec, data)
			pageData = s.page.compressed
		} else {
			pageData = data
		}
		pageHeader = h

	case DataPageHeaderV1:
		if h.IsCompressed(codec) {
			s.page.compressed = makeCompressedPage(s.page.compressed, codec, data)
			pageData = s.page.compressed
		} else {
			pageData = data
		}
		repetitionLevels, definitionLevels, err = s.initDataPageV1(column, pageData)
		if err != nil {
			return fmt.Errorf("initializing v1 reader for page of column %q: %w", columnPath(column.Path()), err)
		}
		pageHeader = h

	default:
		return fmt.Errorf("cannot read values of type %s from page of column %q", h.PageType(), columnPath(column.Path()))
	}

	pageEncoding := pageHeader.Encoding()
	// In some legacy configurations, the PLAIN_DICTIONARY encoding is used on
	// data page headers to indicate that the page contains indexes into the
	// dictionary page, tho it is still encoded using the RLE encoding in this
	// case, so we convert the encoding to RLE_DICTIONARY to simplify.
	switch pageEncoding {
	case format.PlainDictionary:
		pageEncoding = format.RLEDictionary
	}
	s.page.decoder = makeDecoder(s.page.decoder, s.page.encoding, pageEncoding, pageData)
	s.page.encoding = pageEncoding

	maxRepetitionLevel := column.maxRepetitionLevel
	maxDefinitionLevel := column.maxDefinitionLevel
	hasLevels := maxRepetitionLevel > 0 || maxDefinitionLevel > 0
	if hasLevels {
		repetitionLevelEncoding := pageHeader.RepetitionLevelEncoding()
		definitionLevelEncoding := pageHeader.DefinitionLevelEncoding()
		s.repetitions.decoder = makeDecoder(s.repetitions.decoder, s.repetitions.encoding, repetitionLevelEncoding, repetitionLevels)
		s.definitions.decoder = makeDecoder(s.definitions.decoder, s.definitions.encoding, definitionLevelEncoding, definitionLevels)
		s.repetitions.encoding = repetitionLevelEncoding
		s.definitions.encoding = definitionLevelEncoding
	}

	if s.reader == nil {
		bufferSize := defaultReadBufferSize
		if hasLevels {
			bufferSize /= 2
		}
		s.reader = columnType.NewColumnReader(int(column.index), bufferSize)
		if hasLevels {
			s.reader = newFileColumnReader(s.reader, maxRepetitionLevel, maxDefinitionLevel, bufferSize)
		}
	}

	numValues := int(header.NumValues())
	switch r := s.reader.(type) {
	case *fileColumnReader:
		r.reset(numValues, s.repetitions.decoder, s.definitions.decoder, s.page.decoder)
	default:
		r.Reset(numValues, s.page.decoder)
	}

	return nil
}

func (s *filePageValueReaderState) initDataPageV1(column *Column, data io.Reader) (repetitionLevels, definitionLevels io.Reader, err error) {
	s.v1.repetitions.reset()
	s.v1.definitions.reset()

	if column.MaxRepetitionLevel() > 0 {
		if err := s.v1.repetitions.readDataPageV1Level(data, "repetition"); err != nil {
			return nil, nil, err
		}
	}

	if column.MaxDefinitionLevel() > 0 {
		if err := s.v1.definitions.readDataPageV1Level(data, "definition"); err != nil {
			return nil, nil, err
		}
	}

	repetitionLevels = &s.v1.repetitions.section
	definitionLevels = &s.v1.definitions.section
	return repetitionLevels, definitionLevels, nil
}

func (s *filePageValueReaderState) initDataPageV2(header DataPageHeaderV2, data *bytes.Reader) (repetitionLevels, definitionLevels io.Reader, err error) {
	repetitionLevelsByteLength := header.RepetitionLevelsByteLength()
	definitionLevelsByteLength := header.DefinitionLevelsByteLength()

	if repetitionLevelsByteLength > 0 {
		s.v2.repetitions.setDataPageV2Section(data, 0, repetitionLevelsByteLength)
	} else {
		s.v2.repetitions.reset()
	}

	if definitionLevelsByteLength > 0 {
		s.v2.definitions.setDataPageV2Section(data, repetitionLevelsByteLength, definitionLevelsByteLength)
	} else {
		s.v2.definitions.reset()
	}

	if _, err := data.Seek(repetitionLevelsByteLength+definitionLevelsByteLength, io.SeekStart); err != nil {
		return nil, nil, err
	}

	repetitionLevels = &s.v2.repetitions.section
	definitionLevels = &s.v2.definitions.section
	return repetitionLevels, definitionLevels, nil
}

type dataPageLevelV1 struct {
	section bytes.Reader
	buffer  [4]byte
	data    []byte
}

func (lvl *dataPageLevelV1) reset() {
	lvl.section.Reset(nil)
}

// This method breaks abstraction layers a bit, but it is helpful to avoid
// decoding repetition and definition levels if there is no need to.
//
// In the data header format v1, the repetition and definition levels were
// part of the compressed page payload. In order to access the data, the
// levels must be fully read. Because of it, the levels have to be buffered
// to allow the content to be decoded lazily later on.
//
// In the data header format v2, the repetition and definition levels are not
// part of the compressed page data, they can be accessed by slicing a section
// of the file according to the level lengths stored in the column metadata
// header, therefore there is no need to buffer the levels.
func (lvl *dataPageLevelV1) readDataPageV1Level(r io.Reader, typ string) error {
	if _, err := io.ReadFull(r, lvl.buffer[:4]); err != nil {
		return fmt.Errorf("reading RLE encoded length of %s levels: %w", typ, err)
	}

	n := int(binary.LittleEndian.Uint32(lvl.buffer[:4]))
	if cap(lvl.data) < n {
		lvl.data = make([]byte, n)
	} else {
		lvl.data = lvl.data[:n]
	}

	if rn, err := io.ReadFull(r, lvl.data); err != nil {
		return fmt.Errorf("reading %d/%d bytes of %s levels: %w", rn, n, typ, err)
	}

	lvl.section.Reset(lvl.data)
	return nil
}

type dataPageLevelV2 struct {
	section io.SectionReader
}

func (lvl *dataPageLevelV2) reset() {
	lvl.section = *io.NewSectionReader(nil, 0, 0)
}

func (lvl *dataPageLevelV2) setDataPageV2Section(file io.ReaderAt, dataPageOffset, dataPageLength int64) {
	lvl.section = *io.NewSectionReader(file, dataPageOffset, dataPageLength)
}

func makeCompressedPage(page *compressedPageReader, codec format.CompressionCodec, compressed io.Reader) *compressedPageReader {
	if page == nil {
		page = acquireCompressedPageReader(codec, compressed)
	} else {
		if page.codec != codec {
			releaseCompressedPageReader(page)
			page = acquireCompressedPageReader(codec, compressed)
		} else {
			page.Reset(compressed)
		}
	}
	return page
}

func makeDecoder(decoder encoding.Decoder, oldEncoding, newEncoding format.Encoding, input io.Reader) encoding.Decoder {
	if decoder == nil || oldEncoding != newEncoding {
		decoder = LookupEncoding(newEncoding).NewDecoder(input)
	} else {
		decoder.Reset(input)
	}
	return decoder
}

var (
	_ CompressedPage = (*filePage)(nil)

	// This buffer pool is used to optimize memory allocation when scanning
	// through multiple pages of column chunks. Buffers are allocated lazily
	// when reading the first page of a column, then retained until the
	// Pages instance is closed.
	compressedPageBufferPool sync.Pool // *bytes.Buffer
)

func acquireCompressedPageBuffer() *bytes.Buffer {
	b, _ := compressedPageBufferPool.Get().(*bytes.Buffer)
	if b != nil {
		b.Reset()
	} else {
		b = new(bytes.Buffer)
	}
	return b
}

func releaseCompressedPageBuffer(b *bytes.Buffer) {
	if b != nil {
		compressedPageBufferPool.Put(b)
	}
}
