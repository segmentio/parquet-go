package parquet

import (
	"fmt"

	"github.com/segmentio/parquet/format"
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
	metadata *format.ColumnMetaData

	err error
}

func (c *ColumnChunks) close(err error) {
	c.index = len(c.column.chunks)
	c.err = err
}

// Err returns the last error observed by the column chunk iterator.
func (c *ColumnChunks) Err() error {
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

	c.close(fmt.Errorf("remote column data are not supported: %s", chunk.FilePath))
	return false

	// This is a sketch of what the code would look like when we support
	// reading data from remote files.
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
		metadata := new(format.ColumnMetaData)

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
func (c *ColumnChunks) Chunk() *format.ColumnChunk {
	if c.index >= 0 && c.index < len(c.column.chunks) {
		return c.column.chunks[c.index]
	}
	return nil
}

// DataPages returns an iterator over the data pages in the column chunk that
// the iterator is currently positioned at.
func (c *ColumnChunks) Pages() *ColumnPages {
	if c.metadata != nil {
		return newColumnPages(c.column, c.metadata)
	}
	return nil
}
