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

// File represents a parquet file.
type File struct {
	metadata      format.FileMetaData
	protocol      thrift.CompactProtocol
	reader        io.ReaderAt
	size          int64
	root          *Column
	columnIndexes []ColumnIndex
	offsetIndexes []OffsetIndex
	rowGroups     []RowGroup
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
	c := DefaultFileConfig()
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

	schema := NewSchema(f.root.Name(), f.root)
	columns := make([]*Column, 0, MaxColumnIndex+1)
	f.root.forEachLeaf(func(c *Column) { columns = append(columns, c) })

	f.rowGroups = make([]RowGroup, len(f.metadata.RowGroups))
	for i := range f.rowGroups {
		f.rowGroups[i] = newFileRowGroup(f, schema, columns, &f.metadata.RowGroups[i])
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
	section.Reset(f.reader, indexOffset, indexLength)

	for i := range f.metadata.RowGroups {
		for j := range f.metadata.RowGroups[i].Columns {
			n := len(columnIndexes)
			columnIndexes = append(columnIndexes, ColumnIndex{})

			if err := decoder.Decode(&columnIndexes[n]); err != nil {
				return nil, nil, fmt.Errorf("reading column index %d of row group %d: %w", j, i, err)
			}
		}
	}

	for i := range f.metadata.RowGroups {
		for j := range f.metadata.RowGroups[i].Columns {
			n := len(offsetIndexes)
			offsetIndexes = append(offsetIndexes, OffsetIndex{})

			if err := decoder.Decode(&offsetIndexes[n]); err != nil {
				return nil, nil, fmt.Errorf("reading offset index %d of row group %d: %w", j, i, err)
			}
		}
	}

	return columnIndexes, offsetIndexes, nil
}

// ReadRowGroups returns a list containing the row groups in f.
func (f *File) RowGroups() []RowGroup { return f.rowGroups }

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

// Lookup returns the value associated with the given key in the file key/value
// metadata.
//
// The ok boolean will be true if the key was found, false otherwise.
func (f *File) Lookup(key string) (value string, ok bool) {
	return lookupKeyValueMetadata(f.metadata.KeyValueMetadata, key)
}

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

func newFileRowGroup(file *File, schema *Schema, columns []*Column, group *format.RowGroup) RowGroup {
	g := &rowGroup{
		schema:  schema,
		numRows: int(group.NumRows),
		columns: make([]RowGroupColumn, len(group.Columns)),
		sorting: make([]SortingColumn, len(group.SortingColumns)),
	}

	hasIndexes := file.columnIndexes != nil && file.offsetIndexes != nil
	for i := range g.columns {
		c := &fileRowGroupColumn{
			file:   file,
			column: columns[i],
			chunk:  &group.Columns[i],
		}

		if hasIndexes {
			j := (int(group.Ordinal) * len(columns)) + i
			c.columnIndex = &file.columnIndexes[j]
			c.offsetIndex = &file.offsetIndexes[j]
		}

		g.columns[i] = c
	}

	for i := range g.sorting {
		g.sorting[i] = &fileSortingColumn{
			column:     columns[group.SortingColumns[i].ColumnIdx],
			descending: group.SortingColumns[i].Descending,
			nullsFirst: group.SortingColumns[i].NullsFirst,
		}
	}

	return g
}

type fileSortingColumn struct {
	column     *Column
	descending bool
	nullsFirst bool
}

func (s *fileSortingColumn) Path() []string   { return s.column.Path() }
func (s *fileSortingColumn) Descending() bool { return s.descending }
func (s *fileSortingColumn) NullsFirst() bool { return s.nullsFirst }

type fileRowGroupColumn struct {
	file        *File
	column      *Column
	columnIndex *ColumnIndex
	offsetIndex *OffsetIndex
	chunk       *format.ColumnChunk
}

func (c *fileRowGroupColumn) ColumnIndex() int {
	return int(c.column.Index())
}

func (c *fileRowGroupColumn) Dictionary() Dictionary {
	if c.chunk.MetaData.DictionaryPageOffset == 0 {
		return nil
	}
	// TODO
	return nil
}

func (c *fileRowGroupColumn) Pages() PageReader {
	return newFilePageReader(c.file, c.column, c.columnIndex, c.offsetIndex, c.chunk)
}

type filePageReader struct {
	columnIndex *ColumnIndex
	offsetIndex *OffsetIndex
	protocol    thrift.CompactProtocol
	decoder     thrift.Decoder

	section bufferedSectionReader
	// This buffer is used to hold compressed page in memory when they are read;
	// we need to read whole pages because we have to compute the checksum prior
	// to returning the page to the application.
	buffer []byte

	page filePage
}

func newFilePageReader(file *File, column *Column, columnIndex *ColumnIndex, offsetIndex *OffsetIndex, chunk *format.ColumnChunk) *filePageReader {
	r := &filePageReader{
		columnIndex: columnIndex,
		offsetIndex: offsetIndex,
		page: filePage{
			column:     column,
			columnType: column.Type(),
			codec:      chunk.MetaData.Codec,
		},
	}

	pageOffset := chunk.MetaData.DictionaryPageOffset
	if pageOffset == 0 {
		pageOffset = chunk.MetaData.DataPageOffset
	} else {
		r.page.dictionary = r.page.columnType.NewDictionary(0) // TODO: configure buffer size
		r.page.columnType = r.page.dictionary.Type()
	}

	r.section.Reset(file, pageOffset, chunk.MetaData.TotalCompressedSize)
	r.decoder.Reset(r.protocol.NewReader(&r.section))
	return r
}

func (r *filePageReader) ReadPage() (Page, error) {
	for {
		r.page.header = format.PageHeader{}
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
		if cap(r.buffer) < compressedPageSize {
			r.buffer = make([]byte, compressedPageSize)
		} else {
			r.buffer = r.buffer[:compressedPageSize]
		}

		_, err := io.ReadFull(&r.section, r.buffer)
		if err != nil {
			return nil, fmt.Errorf("reading page %d of column %q", r.page.index, r.page.columnPath())
		}

		if r.page.header.CRC != 0 {
			headerChecksum := uint32(r.page.header.CRC)
			bufferChecksum := crc32.ChecksumIEEE(r.buffer)

			if headerChecksum != bufferChecksum {
				return nil, fmt.Errorf("crc32 checksum mismatch in page %d of column %q: 0x%08X != 0x%08X: %w",
					r.page.index,
					r.page.columnPath(),
					headerChecksum,
					bufferChecksum,
					ErrCorrupted,
				)
			}
		}

		r.page.data.Reset(r.buffer)

		if r.page.header.Type != format.DictionaryPage {
			if r.columnIndex != nil {
				err = r.page.parseColumnIndex(r.columnIndex)
			} else {
				err = r.page.parseStatistics()
			}
			r.page.index++
			return &r.page, err
		}

		if r.page.index != 0 {
			return nil, fmt.Errorf("dictionary found after the first page in column chunk of %q", r.page.columnPath())
		}
		if r.page.dictionary == nil {
			return nil, fmt.Errorf("dictionary page found in column %q which had a zero dictionary page offset", r.page.columnPath())
		}

		dict := r.page.dictionary
		dict.Reset()

		enc := r.page.header.DictionaryPageHeader.Encoding
		dec := LookupEncoding(enc).NewDecoder(&r.page.data)

		if err := dict.ReadFrom(dec); err != nil {
			return nil, fmt.Errorf("reading dictionary of column %q", r.page.columnPath())
		}
	}
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

func (p *filePage) parseColumnIndex(columnIndex *ColumnIndex) (err error) {
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

	var stats *format.Statistics
	switch p.header.Type {
	case format.DataPageV2:
		stats = &p.header.DataPageHeaderV2.Statistics
	case format.DataPage:
		stats = &p.header.DataPageHeader.Statistics
	}

	minValue, maxValue, nullPage := columnIndex.PageBounds(p.index)

	if stats != nil {
		if stats.MinValue == nil {
			stats.MinValue = minValue
		}
		if stats.MaxValue == nil {
			stats.MaxValue = maxValue
		}
		if stats.NullCount == 0 {
			stats.NullCount = columnIndex.PageNulls(p.index)
		}
	}

	if nullPage {
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
	var kind = p.columnType.Kind()
	var stats *format.Statistics

	switch p.header.Type {
	case format.DataPageV2:
		stats = &p.header.DataPageHeaderV2.Statistics
	case format.DataPage:
		stats = &p.header.DataPageHeader.Statistics
	}

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

func (p *filePage) ColumnIndex() int {
	return int(p.column.Index())
}

func (p *filePage) Dictionary() Dictionary {
	return p.dictionary
}

func (p *filePage) NumRows() int {
	switch p.header.Type {
	case format.DataPageV2:
		return int(p.header.DataPageHeaderV2.NumRows)
	default:
		return 0
	}
}

func (p *filePage) NumValues() int {
	switch p.header.Type {
	case format.DataPageV2:
		return int(p.header.DataPageHeaderV2.NumValues)
	case format.DataPage:
		return int(p.header.DataPageHeader.NumValues)
	case format.DictionaryPage:
		return int(p.header.DictionaryPageHeader.NumValues)
	default:
		return 0
	}
}

func (p *filePage) NumNulls() int {
	switch p.header.Type {
	case format.DataPageV2:
		return int(p.header.DataPageHeaderV2.NumNulls)
	case format.DataPage:
		return int(p.header.DataPageHeader.Statistics.NullCount)
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
	if err := p.values.reset(p.columnType, p.column, p.codec, p.PageHeader(), &p.data); err != nil {
		return &errorValueReader{err: err}
	}
	return p.values.reader
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

func (p *filePage) PageData() io.Reader {
	return &p.data
}

type filePageValueReaderState struct {
	reader *DataPageReader

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

func (s *filePageValueReaderState) reset(columnType Type, column *Column, codec format.CompressionCodec, header PageHeader, data *bytes.Reader) (err error) {
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
			s.page.compressed = initCompressedPage(s.page.compressed, codec, data)
			pageData = s.page.compressed
		} else {
			pageData = data
		}
		pageHeader = h

	case DataPageHeaderV1:
		if h.IsCompressed(codec) {
			s.page.compressed = initCompressedPage(s.page.compressed, codec, data)
			pageData = s.page.compressed
		} else {
			pageData = data
		}
		repetitionLevels, definitionLevels, err = s.initDataPageV1(column, data)
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
		s.reader = NewDataPageReader(
			columnType,
			column.MaxRepetitionLevel(),
			column.MaxDefinitionLevel(),
			column.Index(),
			defaultReadBufferSize,
		)
	}

	s.reader.Reset(header.NumValues(), s.repetitions.decoder, s.definitions.decoder, s.page.decoder)
	return nil
}

func (s *filePageValueReaderState) initDataPageV1(column *Column, data *bytes.Reader) (repetitionLevels, definitionLevels io.Reader, err error) {
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

var (
	_ CompressedPage = (*filePage)(nil)

	_ io.ByteReader = (*bufferedSectionReader)(nil)
	_ io.Reader     = (*bufferedSectionReader)(nil)
	_ io.WriterTo   = (*bufferedSectionReader)(nil)
)
