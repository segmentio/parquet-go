package parquet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/schema"
)

type ColumnPages struct {
	column   *Column
	metadata *schema.ColumnMetaData
	header   *schema.PageHeader
	reader   *bufio.Reader
	protocol thrift.CompactProtocol
	decoder  thrift.Decoder
	buffer   [4]byte

	data io.LimitedReader
	page *compressedPageReader

	definitions columnLevel
	repetitions columnLevel

	err error
}

func (c *ColumnPages) Close() error {
	c.header = nil

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
		section := io.NewSectionReader(c.column.file, c.metadata.DataPageOffset, c.metadata.TotalCompressedSize)
		c.reader = bufio.NewReaderSize(section, defaultBufferSize)
		c.decoder.Reset(c.protocol.NewReader(c.reader))
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
	c.data.R = c.reader
	c.data.N = int64(header.CompressedPageSize)

	if c.page == nil {
		c.page = acquireCompressedPageReader(c.metadata.Codec, &c.data)
	} else {
		c.page.Reset(&c.data)
	}

	var r = io.Reader(c.page)
	var err error

	switch header.Type {
	case schema.DataPage:
		err = c.readDataPageV1(r)
	case schema.IndexPage:
		// TODO
	case schema.DictionaryPage:
		// TODO
	case schema.DataPageV2:
		// TODO
	}

	if err != nil {
		c.setError(fmt.Errorf("decoding page contents: %w", err))
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

func (c *ColumnPages) readDataPageV1(r io.Reader) error {
	c.repetitions.resetDataPageV1(c.header.DataPageHeader.DefinitionLevelEncoding)
	c.definitions.resetDataPageV1(c.header.DataPageHeader.RepetitionLevelEncoding)
	if c.column.maxRepetitionLevel > 0 {
		if err := c.repetitions.readDataPageV1Level(r, &c.buffer, "repetition"); err != nil {
			return err
		}
	}
	if c.column.maxDefinitionLevel > 0 {
		if err := c.definitions.readDataPageV1Level(r, &c.buffer, "definition"); err != nil {
			return err
		}
	}
	return nil
}

type columnLevel struct {
	encoding schema.Encoding
	data     []byte
	buffer   bytes.Reader
	reader   columnLevelReader
}

type columnLevelReader interface {
	io.Reader
	io.ReaderAt
	Size() int64
}

func (c *columnLevel) reset(encoding schema.Encoding) {
	c.encoding = encoding
	c.data = c.data[:0]
	c.buffer.Reset(c.data)
}

func (c *columnLevel) resetDataPageV1(encoding schema.Encoding) {
	c.reset(encoding)
	c.reader = &c.buffer
}

// This method breaks abstraction layers a bit, but it is helpful to avoid
// decoding repetition and definition levels if there is no need to.
//
// In the data header format v1, the repetition and definition levels were
// part of the compressed page payload. In order to access the data, the
// levels must be fully read. Because of it, the levels have to be buffered
// to allow the content to be decoded lazily layer on.
//
// In the data header format v2, the repetition and definition levels are not
// part of the compressed page data, they can be accessed by slicing a section
// of the file according to the level lengths stored in the column metadata
// header, therefore there is no need to buffer the levels.
func (c *columnLevel) readDataPageV1Level(r io.Reader, buf *[4]byte, typ string) error {
	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return fmt.Errorf("reading RLE encoded length of %s level: %w", typ, err)
	}

	// Work on the assumption that the level is encoded with RLE, in which case
	// the section is prefixed with a 4 byte length of the data.
	m := binary.LittleEndian.Uint32(buf[:4])
	n := int(m) + 4
	if cap(c.data) < n {
		c.data = make([]byte, n)
	} else {
		c.data = c.data[:n]
	}

	if rn, err := io.ReadFull(r, c.data); err != nil {
		return fmt.Errorf("reading %d/%d bytes %s level: %w", rn, m, typ, err)
	}

	// Write the encoded length back to the front of the buffer os the whole
	// datagram remains valid to RLE decoders.
	binary.LittleEndian.PutUint32(c.data[:4], m)
	return nil
}
