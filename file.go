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
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
)

const (
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
	root          *Column
	columnIndexes []format.ColumnIndex
	offsetIndexes []format.OffsetIndex
	rowGroups     []fileRowGroup
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
	section := acquireBufferedSectionReader(r, size-(footerSize+8), footerSize)
	decoder := thrift.NewDecoder(f.protocol.NewReader(section))
	defer releaseBufferedSectionReader(section)

	if err := decoder.Decode(&f.metadata); err != nil {
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

	schema := NewSchema(f.root.Name(), f.root)
	columns := make([]*Column, 0, MaxColumnIndex+1)
	f.root.forEachLeaf(func(c *Column) { columns = append(columns, c) })

	f.rowGroups = make([]fileRowGroup, len(f.metadata.RowGroups))
	for i := range f.rowGroups {
		f.rowGroups[i].init(f, schema, columns, &f.metadata.RowGroups[i])
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
	section := acquireBufferedSectionReader(nil, 0, 0)
	decoder := thrift.NewDecoder(f.protocol.NewReader(section))
	defer releaseBufferedSectionReader(section)
	return f.readPageIndex(section, decoder)
}

func (f *File) readPageIndex(section *bufferedSectionReader, decoder *thrift.Decoder) ([]format.ColumnIndex, []format.OffsetIndex, error) {
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
	columnIndexes := make([]format.ColumnIndex, 0, numColumnChunks)
	offsetIndexes := make([]format.OffsetIndex, 0, numColumnChunks)
	section.Reset(f.reader, indexOffset, indexLength)

	for i := range f.metadata.RowGroups {
		for j := range f.metadata.RowGroups[i].Columns {
			n := len(columnIndexes)
			columnIndexes = append(columnIndexes, format.ColumnIndex{})

			if err := decoder.Decode(&columnIndexes[n]); err != nil {
				return nil, nil, fmt.Errorf("reading column index %d of row group %d: %w", j, i, err)
			}
		}
	}

	for i := range f.metadata.RowGroups {
		for j := range f.metadata.RowGroups[i].Columns {
			n := len(offsetIndexes)
			offsetIndexes = append(offsetIndexes, format.OffsetIndex{})

			if err := decoder.Decode(&offsetIndexes[n]); err != nil {
				return nil, nil, fmt.Errorf("reading offset index %d of row group %d: %w", j, i, err)
			}
		}
	}

	return columnIndexes, offsetIndexes, nil
}

// NumRowGroups returns the number of row groups in f.
func (f *File) NumRowGroups() int { return len(f.rowGroups) }

// RowGroup returns the row group at the given index in f.
func (f *File) RowGroup(i int) RowGroup { return &f.rowGroups[i] }

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

func (f *File) readColumnIndex(chunk *format.ColumnChunk) (*format.ColumnIndex, error) {
	columnIndex := new(format.ColumnIndex)

	section := acquireBufferedSectionReader(f.reader, chunk.ColumnIndexOffset, int64(chunk.ColumnIndexLength))
	defer releaseBufferedSectionReader(section)

	err := thrift.NewDecoder(f.protocol.NewReader(section)).Decode(columnIndex)
	return columnIndex, err
}

func (f *File) readOffsetIndex(chunk *format.ColumnChunk) (*format.OffsetIndex, error) {
	offsetIndex := new(format.OffsetIndex)

	section := acquireBufferedSectionReader(f.reader, chunk.OffsetIndexOffset, int64(chunk.OffsetIndexLength))
	defer releaseBufferedSectionReader(section)

	err := thrift.NewDecoder(f.protocol.NewReader(section)).Decode(offsetIndex)
	return offsetIndex, err
}

func (f *File) hasIndexes() bool {
	return f.columnIndexes != nil && f.offsetIndexes != nil
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
	b.Reader = *bufio.NewReaderSize(&b.section, defaultReadBufferSize)
	return b
}

func (b *bufferedSectionReader) Reset(r io.ReaderAt, offset, length int64) {
	b.section = *io.NewSectionReader(r, offset, length)
	b.Reader.Reset(&b.section)
}

func acquireBufferedSectionReader(r io.ReaderAt, offset, length int64) *bufferedSectionReader {
	b, _ := bufferedSectionReaderPool.Get().(*bufferedSectionReader)
	if b == nil {
		b = newBufferedSectionReader(r, offset, length)
	} else {
		b.Reset(r, offset, length)
	}
	return b
}

func releaseBufferedSectionReader(b *bufferedSectionReader) {
	b.Reset(nil, 0, 0)
	bufferedSectionReaderPool.Put(b)
}

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
	columns  []fileColumnChunk
	sorting  []SortingColumn
}

func (g *fileRowGroup) init(file *File, schema *Schema, columns []*Column, rowGroup *format.RowGroup) {
	g.schema = schema
	g.rowGroup = rowGroup
	g.columns = make([]fileColumnChunk, len(rowGroup.Columns))
	g.sorting = make([]SortingColumn, len(rowGroup.SortingColumns))

	for i := range g.columns {
		c := fileColumnChunk{
			file:     file,
			column:   columns[i],
			rowGroup: rowGroup,
			chunk:    &rowGroup.Columns[i],
		}

		if file.hasIndexes() {
			j := (int(rowGroup.Ordinal) * len(columns)) + i
			c.columnIndex = &file.columnIndexes[j]
			c.offsetIndex = &file.offsetIndexes[j]
		}

		g.columns[i] = c
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
func (g *fileRowGroup) NumColumns() int                 { return len(g.columns) }
func (g *fileRowGroup) Column(i int) ColumnChunk        { return &g.columns[i] }
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
	baseOffset := c.chunk.MetaData.DataPageOffset
	r.dataOffset = baseOffset
	if c.chunk.MetaData.DictionaryPageOffset != 0 {
		baseOffset = c.chunk.MetaData.DictionaryPageOffset
		r.dictOffset = baseOffset
	}
	r.section = io.NewSectionReader(c.file, baseOffset, c.chunk.MetaData.TotalCompressedSize)
	r.rbuf = bufio.NewReaderSize(r.section, defaultReadBufferSize)
	r.section.Seek(r.dataOffset-baseOffset, io.SeekStart)
	r.decoder.Reset(r.protocol.NewReader(r.rbuf))
}

func (c *fileColumnChunk) ColumnIndex() ColumnIndex {
	if c.columnIndex == nil {
		return nil
	}
	return (*columnIndex)(c.columnIndex)
}

func (c *fileColumnChunk) OffsetIndex() OffsetIndex {
	if c.offsetIndex == nil {
		return nil
	}
	return (*offsetIndex)(c.offsetIndex)
}

type filePages struct {
	column     *fileColumnChunk
	protocol   thrift.CompactProtocol
	decoder    thrift.Decoder
	dictOffset int64
	dataOffset int64

	section *io.SectionReader
	rbuf    *bufio.Reader

	// This buffer holds compressed pages in memory when they are read; we need
	// to read whole pages because we have to compute the checksum prior to
	// exposing the page to the application.
	compressedPageData []byte

	page filePage
	skip int64
}

func (r *filePages) readPage() (*filePage, error) {
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
		*h.DictionaryPageHeader = format.DictionaryPageHeader{}
	}
	if h.DataPageHeaderV2 != nil {
		*h.DataPageHeaderV2 = format.DataPageHeaderV2{}
	}

	if err := r.decoder.Decode(&r.page.header); err != nil {
		if err != io.EOF {
			err = fmt.Errorf("decoding page header: %w", err)
		}
		if r.page.values != nil {
			r.page.values.release()
			r.page.values = nil
		}
		return nil, err
	}

	compressedPageSize := int(r.page.header.CompressedPageSize)
	if cap(r.compressedPageData) < compressedPageSize {
		r.compressedPageData = make([]byte, compressedPageSize)
	} else {
		r.compressedPageData = r.compressedPageData[:compressedPageSize]
	}

	_, err := io.ReadFull(r.rbuf, r.compressedPageData)
	if err != nil {
		return nil, fmt.Errorf("reading page %d of column %q", r.page.index, r.page.columnPath())
	}

	if r.page.header.CRC != 0 {
		headerChecksum := uint32(r.page.header.CRC)
		bufferChecksum := crc32.ChecksumIEEE(r.compressedPageData)

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

	r.page.data.Reset(r.compressedPageData)

	if r.column.columnIndex != nil {
		err = r.page.parseColumnIndex(r.column.columnIndex)
	} else {
		err = r.page.parseStatistics()
	}
	return &r.page, err
}

func (r *filePages) readDictionary() error {
	r.page.dictionary = r.page.columnType.NewDictionary(0) // TODO: configure buffer size
	r.page.columnType = r.page.dictionary.Type()
	currentOffset, _ := r.section.Seek(0, io.SeekCurrent)
	defer func() {
		r.section.Seek(currentOffset, io.SeekStart)
		r.rbuf.Reset(r.section)
	}()

	if _, err := r.section.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seeking to dictionary page offset: %w", err)
	}
	r.rbuf.Reset(r.section)

	p, err := r.readPage()
	if err != nil {
		return err
	}

	dict := r.page.dictionary
	page := acquireCompressedPageReader(p.codec, &p.data)

	enc := r.page.header.DictionaryPageHeader.Encoding
	dec := LookupEncoding(enc).NewDecoder(page)

	dict.Reset()
	err = dict.ReadFrom(dec)
	releaseCompressedPageReader(page)

	if err != nil {
		return fmt.Errorf("reading dictionary of column %q: %w", p.columnPath(), err)
	}
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
		_, err = r.section.Seek(r.dataOffset, io.SeekStart)
		r.skip = rowIndex
		r.page.index = 0
	} else {
		pages := r.column.offsetIndex.PageLocations
		index := sort.Search(len(pages), func(i int) bool {
			return pages[i].FirstRowIndex >= rowIndex
		})

		switch {
		case index == 0:
			_, err = r.section.Seek(r.dataOffset, io.SeekStart)
			r.skip = rowIndex
			r.page.index = 0

		case index == len(pages):
			_, err = r.section.Seek(0, io.SeekEnd)
			r.skip = 0
			r.page.index = len(pages)

		case pages[index].FirstRowIndex == rowIndex:
			_, err = r.section.Seek(pages[index].Offset-r.dataOffset, io.SeekStart)
			r.skip = rowIndex - pages[index].FirstRowIndex
			r.page.index = index

		default:
			_, err = r.section.Seek(pages[index-1].Offset-r.dataOffset, io.SeekStart)
			r.skip = rowIndex - pages[index-1].FirstRowIndex
			r.page.index = index - 1
		}
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

	index    int
	minValue Value
	maxValue Value

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
	errPageHasNoColumnIndexNorStatistics     = errors.New("column has no index and page has no statistics")
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
	}

	return nil
}

func (p *filePage) parseStatistics() (err error) {
	kind := p.columnType.Kind()
	stats := p.statistics()

	if stats == nil {
		return p.errStatistics(errPageHasNoColumnIndexNorStatistics)
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

func (p *filePage) Bounds() (min, max Value) {
	return p.minValue, p.maxValue
}

func (p *filePage) Size() int64 {
	return int64(p.header.UncompressedPageSize)
}

func (p *filePage) Values() ValueReader {
	if p.values == nil {
		p.values = new(filePageValueReaderState)
	}
	if err := p.values.init(p.columnType, p.column, p.codec, p.PageHeader(), &p.data); err != nil {
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
	reader *dataPageReader

	v1 struct {
		repetitions dataPageLevelV1
		definitions dataPageLevelV1
	}

	v2 struct {
		repetitions dataPageLevelV2
		definitions dataPageLevelV2
	}

	repetitions struct {
		decoder encoding.Decoder
	}

	definitions struct {
		decoder encoding.Decoder
	}

	page struct {
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

func (s *filePageValueReaderState) init(columnType Type, column *Column, codec format.CompressionCodec, header PageHeader, data *bytes.Reader) (err error) {
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
		return fmt.Errorf("cannot read values from page of type %s", h.PageType())
	}

	s.repetitions.decoder = makeDecoder(s.repetitions.decoder, pageHeader.RepetitionLevelEncoding(), repetitionLevels)
	s.definitions.decoder = makeDecoder(s.definitions.decoder, pageHeader.DefinitionLevelEncoding(), definitionLevels)
	s.page.decoder = makeDecoder(s.page.decoder, pageHeader.Encoding(), pageData)

	if s.reader == nil {
		s.reader = newDataPageReader(
			columnType,
			column.MaxRepetitionLevel(),
			column.MaxDefinitionLevel(),
			column.Index(),
			defaultReadBufferSize,
		)
	}

	// TODO: revisit the data page reader APIs
	s.reader.Reset(int(header.NumValues()), s.repetitions.decoder, s.definitions.decoder, s.page.decoder)
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

func makeDecoder(decoder encoding.Decoder, encoding format.Encoding, input io.Reader) encoding.Decoder {
	if decoder == nil || encoding != decoder.Encoding() {
		decoder = LookupEncoding(encoding).NewDecoder(input)
	} else {
		decoder.Reset(input)
	}
	return decoder
}

var (
	_ CompressedPage = (*filePage)(nil)

	_ io.ByteReader = (*bufferedSectionReader)(nil)
	_ io.Reader     = (*bufferedSectionReader)(nil)
	_ io.WriterTo   = (*bufferedSectionReader)(nil)
)
