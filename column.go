package parquet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/bits"
	"sort"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/schema"
)

// Column represents a column in a parquet file.
//
// Methods of Column values are safe to call concurrently from multiple
// goroutines.
type Column struct {
	file    *File
	schema  *schema.SchemaElement
	order   *schema.ColumnOrder
	columns []*Column
	chunks  []*schema.ColumnChunk

	maxDefinitionLevel uint32
	maxRepetitionLevel uint32
}

// Required returns true if the column is required.
func (c *Column) Required() bool {
	return c.schema.RepetitionType == schema.Required
}

// Optional returns true if the column is optional.
func (c *Column) Optional() bool {
	return c.schema.RepetitionType == schema.Optional
}

// Repeated returns true if the column may repeat.
func (c *Column) Repeated() bool {
	return c.schema.RepetitionType == schema.Repeated
}

// String returns a human-redable string representation of the oclumn.
func (c *Column) String() string {
	switch {
	case c.columns != nil:
		return fmt.Sprintf("%s{%s}",
			c.schema.Name,
			c.schema.RepetitionType)

	case c.schema.Type == schema.FixedLenByteArray:
		return fmt.Sprintf("%s{%s(%d),%s}",
			c.schema.Name,
			c.schema.Type,
			c.schema.TypeLength,
			c.schema.RepetitionType)

	default:
		return fmt.Sprintf("%s{%s,%s}",
			c.schema.Name,
			c.schema.Type,
			c.schema.RepetitionType)
	}
}

// Name returns the column name.
func (c *Column) Name() string {
	return c.schema.Name
}

// Columns returns the list of child columns.
//
// The method returns the same slice across multiple calls, the program must
// treat it as a read-only value.
func (c *Column) Columns() []*Column {
	return c.columns
}

// Column returns the child column matching the given name.
func (c *Column) Column(name string) *Column {
	i := sort.Search(len(c.columns), func(i int) bool {
		return c.columns[i].Name() >= name
	})
	if i < len(c.columns) && c.columns[i].Name() == name {
		return c.columns[i]
	}
	return nil
}

// Scan returns a row iterator which can be used to scan through rows of
// of the column.
//func (c *Column) Scan() *ColumnRows {
//	return &ColumnRows{column: c}
//}

// Chunks returns an iterator over the column chunks that compose this column.
func (c *Column) Chunks() *ColumnChunks {
	return &ColumnChunks{
		file:               c.file,
		size:               c.file.Size(),
		chunks:             c.chunks,
		index:              -1,
		maxDefinitionLevel: c.maxDefinitionLevel,
		maxRepetitionLevel: c.maxRepetitionLevel,
	}
}

func openColumns(file *File) (*Column, error) {
	cl := columnLoader{}

	c, err := cl.open(file)
	if err != nil {
		return nil, err
	}

	// Validate that there aren't extra entries in the row group columns,
	// which would otherwise indicate that there are dangling data pages
	// in the file.
	for index, rowGroup := range file.metadata.RowGroups {
		if cl.rowGroupColumnIndex != len(rowGroup.Columns) {
			return nil, fmt.Errorf("row group at index %d contains %d columns but %d were referenced by the column schemas",
				index, len(rowGroup.Columns), cl.rowGroupColumnIndex)
		}
	}

	setMaxLevels(c, 0)
	return c, nil
}

func setMaxLevels(col *Column, level uint32) {
	col.maxRepetitionLevel = level
	col.maxDefinitionLevel = level

	if col.schema.RepetitionType != schema.Required {
		level++
	}

	for _, c := range col.columns {
		setMaxLevels(c, level)
	}
}

type columnLoader struct {
	schemaIndex         int
	columnOrderIndex    int
	rowGroupColumnIndex int
}

func (cl *columnLoader) open(file *File) (*Column, error) {
	c := &Column{
		file:   file,
		schema: &file.metadata.Schema[cl.schemaIndex],
	}

	cl.schemaIndex++
	numChildren := int(c.schema.NumChildren)

	if numChildren == 0 {
		if cl.columnOrderIndex < len(file.metadata.ColumnOrders) {
			c.order = &file.metadata.ColumnOrders[cl.columnOrderIndex]
			cl.columnOrderIndex++
		}

		c.chunks = make([]*schema.ColumnChunk, 0, len(file.metadata.RowGroups))
		for index, rowGroup := range file.metadata.RowGroups {
			if cl.rowGroupColumnIndex >= len(rowGroup.Columns) {
				return nil, fmt.Errorf("row group at index %d does not have enough columns", index)
			}
			c.chunks = append(c.chunks, &rowGroup.Columns[cl.rowGroupColumnIndex])
		}
		cl.rowGroupColumnIndex++

		return c, nil
	}

	c.columns = make([]*Column, numChildren)

	for i := range c.columns {
		if cl.schemaIndex >= len(file.metadata.Schema) {
			return nil, fmt.Errorf("column %q has more children than there are schemas in the file: %d > %d",
				c.schema.Name, cl.schemaIndex+1, len(file.metadata.Schema))
		}

		var err error
		c.columns[i], err = cl.open(file)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.schema.Name, err)
		}
	}

	return c, nil
}

// ColumnChunks is an iterator type exposing chunks of a column within a parquet
// file.
type ColumnChunks struct {
	file   io.ReaderAt
	size   int64
	index  int
	chunks []*schema.ColumnChunk

	// reader   *io.SectionReader
	// buffer   *bufio.Reader
	// protocol thrift.CompactProtocol
	// decoder  thrift.Decoder
	metadata *schema.ColumnMetaData

	maxDefinitionLevel uint32
	maxRepetitionLevel uint32

	err error
}

// Close closes the iterator, positioning it at the end of the column chunk
// sequence, and returns the last error it ecountered.
func (c *ColumnChunks) Close() error {
	c.index = len(c.chunks)
	c.metadata = nil
	return c.err
}

// Seek positions the iterator at the given index. The program must still call
// Next after calling Seek, otherwise the
func (c *ColumnChunks) Seek(index int) {
	c.index = index - 1
	c.metadata = nil
}

// Next advances the iterator to the next chunk.
func (c *ColumnChunks) Next() bool {
	c.metadata = nil

	if c.index++; c.index >= len(c.chunks) {
		return false
	}
	chunk := c.chunks[c.index]

	if chunk.FilePath == "" {
		c.metadata = &chunk.MetaData
		return true
	}

	c.setError(fmt.Errorf("remote column data are not supported: %s", chunk.FilePath))
	return false

	/*
		if c.reader == nil {
			c.reader = io.NewSectionReader(c.file, 0, c.size)
			c.buffer = bufio.NewReaderSize(c.reader, defaultBufferSize)
			c.decoder.Reset(c.protocol.NewReader(c.buffer))
		}

		if _, err := c.reader.Seek(chunk.FileOffset, io.SeekStart); err != nil {
			c.setError(err)
			return false
		}

		c.buffer.Reset(c.reader)
		metadata := new(schema.ColumnMetaData)

		if err := c.decoder.Decode(metadata); err != nil {
			c.setError(err)
			return false
		}

		c.metadata = metadata
		return true
	*/
}

// Chunk returns the schema for the chunk that the iterator is currently
// positioned at. The method returns nil after the iterator reached the end or
// encountered an error.
func (c *ColumnChunks) Chunk() *schema.ColumnChunk {
	if c.index >= 0 && c.index < len(c.chunks) {
		return c.chunks[c.index]
	}
	return nil
}

// MetaData returns the column metadata for the chunk that the iterator is
// currently positioned at. The method returns nil after the iterator reached
// the end or encountered an error.
func (c *ColumnChunks) MetaData() *schema.ColumnMetaData {
	return c.metadata
}

// DataPages returns an iterator over the data pages in the column chunk that
// the iterator is currently positioned at.
func (c *ColumnChunks) DataPages() *ColumnPages {
	if c.metadata != nil {
		return &ColumnPages{
			file:               c.file,
			size:               c.size,
			metadata:           c.metadata,
			maxDefinitionLevel: c.maxDefinitionLevel,
			maxRepetitionLevel: c.maxRepetitionLevel,
		}
	}
	return nil
}

func (c *ColumnChunks) setError(err error) {
	c.index = len(c.chunks)
	c.err = err
}

type ColumnPages struct {
	file     io.ReaderAt
	size     int64
	metadata *schema.ColumnMetaData
	header   *schema.PageHeader

	reader   *io.SectionReader
	buffer   *bufio.Reader
	protocol thrift.CompactProtocol
	decoder  thrift.Decoder

	data io.LimitedReader
	page *compressedPageReader

	maxDefinitionLevel uint32
	maxRepetitionLevel uint32

	definitionLevelDecoder encoding.Int32Decoder
	repetitionLevelDecoder encoding.Int32Decoder

	definitionLevelEncoding schema.Encoding
	repetitionLevelEncoding schema.Encoding

	definitionLevels []int32
	repetitionLevels []int32

	err error
}

func (c *ColumnPages) Close() error {
	c.header = nil

	closeLevelDecoderIfNotNil(c.definitionLevelDecoder)
	closeLevelDecoderIfNotNil(c.repetitionLevelDecoder)
	c.definitionLevelDecoder = nil
	c.repetitionLevelDecoder = nil

	if c.page != nil {
		releaseCompressedPageReader(c.page)
		c.page = nil
	}

	switch {
	case c.err == nil, errors.Is(c.err, io.EOF):
		return nil
	default:
		return c.err
	}
}

func (c *ColumnPages) Next() bool {
	if c.err != nil {
		return false
	}

	if c.reader == nil {
		c.reader = io.NewSectionReader(c.file, c.metadata.DataPageOffset, c.metadata.TotalCompressedSize)
		c.buffer = bufio.NewReaderSize(c.reader, defaultBufferSize)
		c.decoder.Reset(c.protocol.NewReader(c.buffer))
	}

	if c.data.N > 0 {
		if _, err := io.Copy(ioutil.Discard, &c.data); err != nil {
			c.setError(fmt.Errorf("skipping unread page data: %w", err))
			return false
		}
	}

	header := new(schema.PageHeader)
	if err := c.decoder.Decode(header); err != nil {
		c.setError(fmt.Errorf("decoding page header: %w", err))
		return false
	}

	c.header = header
	c.data.R = c.buffer
	c.data.N = int64(header.CompressedPageSize)

	if c.page == nil {
		c.page = acquireCompressedPageReader(c.metadata.Codec, &c.data)
	} else {
		c.page.Reset(&c.data)
	}

	//r := &debugReader{c.page}
	r := io.Reader(c.page)

	c.definitionLevelDecoder = resetLevelDecoder(r, c.definitionLevelDecoder, c.definitionLevelEncoding, header.DataPageHeader.DefinitionLevelEncoding)
	c.repetitionLevelDecoder = resetLevelDecoder(r, c.repetitionLevelDecoder, c.repetitionLevelEncoding, header.DataPageHeader.RepetitionLevelEncoding)
	c.definitionLevelEncoding = header.DataPageHeader.DefinitionLevelEncoding
	c.repetitionLevelEncoding = header.DataPageHeader.RepetitionLevelEncoding
	c.definitionLevels = resizeLevels(c.definitionLevels, int(header.DataPageHeader.NumValues))
	c.repetitionLevels = resizeLevels(c.repetitionLevels, int(header.DataPageHeader.NumValues))
	var err error

	c.repetitionLevels, err = decodeLevels(c.repetitionLevelDecoder, c.repetitionLevels, c.maxDefinitionLevel)
	if err != nil {
		c.setError(fmt.Errorf("decoding repetition levels: %w", err))
		return false
	}

	c.definitionLevels, err = decodeLevels(c.definitionLevelDecoder, c.definitionLevels, c.maxRepetitionLevel)
	if err != nil {
		c.setError(fmt.Errorf("decoding definition levels: %w", err))
		return false
	}

	return true
}

func (c *ColumnPages) Header() *schema.PageHeader {
	return c.header
}

func (c *ColumnPages) setError(err error) {
	c.header, c.err = nil, err
}

type debugReader struct {
	r io.Reader
}

func (d *debugReader) Read(b []byte) (int, error) {
	n, err := d.r.Read(b)
	fmt.Printf("Read: %[1]x %08[1]b\n", b[:n])
	return n, err
}

func closeLevelDecoderIfNotNil(decoder encoding.Int32Decoder) {
	if decoder != nil {
		decoder.Close()
	}
}

func decodeLevels(decoder encoding.Int32Decoder, levels []int32, maxLevel uint32) ([]int32, error) {
	decoder.SetBitWidth(bits.Len32(maxLevel))
	n, err := decoder.DecodeInt32(levels)
	if err != nil && err != io.EOF {
		return levels[:0], err
	}
	return levels[:n], nil
}

func resetLevelDecoder(r io.Reader, decoder encoding.Int32Decoder, oldEncoding, newEncoding schema.Encoding) encoding.Int32Decoder {
	switch {
	case decoder == nil:
		return lookupEncoding(newEncoding).NewInt32Decoder(r)
	case oldEncoding != newEncoding:
		decoder.Close()
		return lookupEncoding(newEncoding).NewInt32Decoder(r)
	default:
		decoder.Reset(r)
		return decoder
	}
}

func resizeLevels(slice []int32, size int) []int32 {
	if cap(slice) < size {
		return make([]int32, size)
	} else {
		return slice[:size]
	}
}
