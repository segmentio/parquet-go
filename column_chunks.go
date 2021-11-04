package parquet

import (
	"fmt"

	"github.com/segmentio/parquet/schema"
)

// ColumnChunks is an iterator type exposing chunks of a column within a parquet
// file.
type ColumnChunks struct {
	column *Column
	index  int

	// reader   *io.SectionReader
	// buffer   *bufio.Reader
	// protocol thrift.CompactProtocol
	// decoder  thrift.Decoder
	metadata *schema.ColumnMetaData

	err error
}

// Close closes the iterator, positioning it at the end of the column chunk
// sequence, and returns the last error it ecountered.
func (c *ColumnChunks) Close() error {
	c.index = len(c.column.chunks)
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

	if c.index++; c.index >= len(c.column.chunks) {
		return false
	}
	chunk := c.column.chunks[c.index]

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
	if c.index >= 0 && c.index < len(c.column.chunks) {
		return c.column.chunks[c.index]
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
			column:   c.column,
			metadata: c.metadata,
		}
	}
	return nil
}

func (c *ColumnChunks) setError(err error) {
	c.index = len(c.column.chunks)
	c.err = err
}
