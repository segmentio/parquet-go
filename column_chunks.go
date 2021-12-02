package parquet

import (
	"bufio"
	"fmt"
	"io"

	"github.com/segmentio/parquet/format"
)

// ColumnChunks is an iterator type exposing chunks of a column within a parquet
// file.
type ColumnChunks struct {
	column      *Column
	index       int
	metadata    *format.ColumnMetaData
	columnIndex *ColumnIndex
	offsetIndex *OffsetIndex
	buffer      *bufio.Reader
	err         error
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
// Next after calling Seek, otherwise the behavior is undefined.
func (c *ColumnChunks) Seek(index int) {
	c.index = index - 1
	c.metadata = nil
	c.columnIndex = nil
	c.offsetIndex = nil
}

// Next advances the iterator to the next chunk.
func (c *ColumnChunks) Next() bool {
	c.metadata = nil
	c.columnIndex = nil
	c.offsetIndex = nil

	if c.index++; c.index >= len(c.column.chunks) {
		return false
	}
	chunk := c.column.chunks[c.index]

	if len(c.column.columnIndex) != 0 {
		c.columnIndex = c.column.columnIndex[c.index]
	}

	if len(c.column.offsetIndex) != 0 {
		c.offsetIndex = c.column.offsetIndex[c.index]
	}

	if chunk.FilePath == "" {
		c.metadata = &chunk.MetaData
		return true
	}

	c.close(fmt.Errorf("remote column data are not supported: %s", chunk.FilePath))
	return false
}

// DataPages returns an iterator over the data pages in the column chunk that
// the iterator is currently positioned at.
func (c *ColumnChunks) Pages() *ColumnPages {
	if c.metadata != nil {
		return newColumnPages(c.column, c.metadata, c.columnIndex, c.offsetIndex, defaultBufferSize)
	}
	return nil
}

// ReadColumnIndex reads the column index section of the column chunk.
func (c *ColumnChunks) ReadColumnIndex() (*ColumnIndex, error) {
	chunk := c.chunk()
	if chunk == nil {
		return nil, io.EOF
	}
	return c.column.file.readColumnIndex(chunk)
}

// ReadOffsetIndex reads the offset index section of the column chunk.
func (c *ColumnChunks) ReadOffsetIndex() (*OffsetIndex, error) {
	chunk := c.chunk()
	if chunk == nil {
		return nil, io.EOF
	}
	return c.column.file.readOffsetIndex(chunk)
}

func (c *ColumnChunks) chunk() *format.ColumnChunk {
	if c.index >= 0 && c.index < len(c.column.chunks) {
		return c.column.chunks[c.index]
	}
	return nil
}
