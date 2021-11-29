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
	"github.com/segmentio/parquet/format"
)

var (
	// Corrupted is an error returned by the Err method of ColumnPages instances
	// when they encountered a mismatch between the CRC checksum recorded in a
	// page header and the one computed while reading the page data.
	Corrupted = errors.New("corrupted")
)

// ColumnPages is an iterator type used to scan pages of a column chunk.
type ColumnPages struct {
	column   *Column
	header   *format.PageHeader
	section  offsetReader
	crc32    crc32Reader
	codec    format.CompressionCodec
	protocol thrift.CompactProtocol
	decoder  thrift.Decoder

	data               io.LimitedReader
	page               *compressedPageReader
	repetitionLevels   io.Reader
	definitionLevels   io.Reader
	compressedPageData io.Reader
	pageData           io.Reader
	pageHeader         PageHeader

	v1 struct {
		repetitions dataPageLevelV1
		definitions dataPageLevelV1
	}

	v2 struct {
		repetitions dataPageLevelV2
		definitions dataPageLevelV2
	}

	err error
}

func newColumnPages(column *Column, metadata *format.ColumnMetaData, bufferSize int) *ColumnPages {
	c := &ColumnPages{
		column: column,
		codec:  metadata.Codec,
	}
	pageOffset := metadata.DataPageOffset
	if metadata.DictionaryPageOffset > 0 {
		pageOffset = metadata.DictionaryPageOffset
	}
	fileSection := io.NewSectionReader(column.file, pageOffset, metadata.TotalCompressedSize)
	c.section.reader = bufio.NewReaderSize(fileSection, bufferSize)
	c.section.offset = pageOffset
	c.decoder.Reset(c.protocol.NewReader(&c.section))
	return c
}

func (c *ColumnPages) close(err error) {
	if c.page != nil {
		releaseCompressedPageReader(c.page)
		c.page = nil
	}
	c.header = nil
	c.data.R = nil
	c.data.N = 0
	c.err = err
}

// Err return the last error seen by the column page iterator.
//
// Note that not all errors are fatal; page corruption errors can be handled by
// skipping the page and allowing the program to continue reading the remaining
// valid pages in the column chunk.
func (c *ColumnPages) Err() error {
	switch c.err {
	case nil:
	case io.EOF:
		return nil
	default:
		return c.err
	}

	// Only if all the current page data have been consumed, and if the current
	// page hader have a non-zero CRC32 checksum, tests that the checksums are
	// equal.
	if c.header != nil && c.header.CRC != 0 && c.data.N == 0 {
		headerChecksum := uint32(c.header.CRC)
		readerChecksum := c.crc32.Sum32()

		if headerChecksum != readerChecksum {
			return fmt.Errorf("crc32 checksum mismatch: 0x%08X != 0x%08X: %w", headerChecksum, readerChecksum, Corrupted)
		}
	}

	return nil
}

// Next advances the iterator to the next page, returning true if another page
// was available.
//
// When newly created, the iterator is positioned before the first page,
// therefore Next must be called before any other method of the iterator.
func (c *ColumnPages) Next() bool {
	if c.data.N > 0 {
		if _, err := io.Copy(ioutil.Discard, &c.data); err != nil {
			c.close(fmt.Errorf("skipping unread page data: %w", err))
			return false
		}
	}

	header := new(format.PageHeader)
	if err := c.decoder.Decode(header); err != nil {
		if err != io.EOF {
			err = fmt.Errorf("decoding page header: %w", err)
		}
		c.close(err)
		return false
	}

	reader := io.Reader(&c.section)
	if header.CRC != 0 {
		c.crc32.Reset(reader)
		reader = &c.crc32
	}

	c.header = header
	c.data.R = reader
	c.data.N = int64(header.CompressedPageSize)

	switch header.Type {
	case format.DictionaryPage:
		c.repetitionLevels = emptyReader{}
		c.definitionLevels = emptyReader{}
		if c.codec == format.Uncompressed {
			c.pageData = &c.data
		} else {
			c.page = initCompressedPage(c.page, c.codec, &c.data)
			c.pageData = c.page
		}
		c.compressedPageData = &c.data
		c.pageHeader = DictionaryPageHeader{header.DictionaryPageHeader}

	case format.DataPage:
		if c.codec == format.Uncompressed {
			c.pageData = &c.data
		} else {
			c.page = initCompressedPage(c.page, c.codec, &c.data)
			c.pageData = c.page
		}
		c.err = c.initDataPageV1(c.pageData)
		c.compressedPageData = c.pageData
		c.pageHeader = DataPageHeaderV1{header.DataPageHeader}

	case format.DataPageV2:
		c.err = c.initDataPageV2(header)
		if c.err != nil {
			c.pageData = &c.data
		} else {
			codec := format.Uncompressed
			if header.DataPageHeaderV2.IsCompressed == nil || *header.DataPageHeaderV2.IsCompressed {
				codec = c.codec
			}
			if codec == format.Uncompressed {
				c.pageData = &c.data
			} else {
				c.page = initCompressedPage(c.page, codec, &c.data)
				c.pageData = c.page
			}
		}
		c.compressedPageData = &c.data
		c.pageHeader = DataPageHeaderV2{header.DataPageHeaderV2}

	default:
		c.repetitionLevels = emptyReader{}
		c.definitionLevels = emptyReader{}
		c.compressedPageData = &c.data
		c.pageData = &c.data
		c.pageHeader = unknownPageHeader{header}
		c.err = fmt.Errorf("cannot decode page of type %s", header.Type)
	}

	return true
}

// RepetitionLevels returns an io.Reader exposing the raw bytes of the
// repetition levels in the current data page.
func (c *ColumnPages) RepetitionLevels() io.Reader { return c.repetitionLevels }

// DefinitionLevels returns an io.Reader exposing the raw bytes of the
// definition levels in the current data page.
func (c *ColumnPages) DefinitionLevels() io.Reader { return c.definitionLevels }

// CompressedDataPage returns an io.Reader exposing the raw bytes of the
// current page before decompression.
//
// This method is useful to avoid decompressing data pages when doing low-level
// manipulations of parquet files.
func (c *ColumnPages) CompressedPageData() io.Reader { return c.compressedPageData }

// PageData returns an io.Reader exposing the raw bytes of the current page
// after decompression.
func (c *ColumnPages) PageData() io.Reader { return c.pageData }

// PageHeader returns a PageHeader representing the header of the current page.
func (c *ColumnPages) PageHeader() PageHeader { return c.pageHeader }

func (c *ColumnPages) fileOffset() int64 {
	return c.section.offset
}

func (c *ColumnPages) initDataPageV1(r io.Reader) error {
	maxRepetitionLevel := c.column.maxRepetitionLevel
	maxDefinitionLevel := c.column.maxDefinitionLevel
	c.v1.repetitions.reset()
	c.v1.definitions.reset()

	if maxRepetitionLevel > 0 {
		if err := c.v1.repetitions.readDataPageV1Level(r, "repetition"); err != nil {
			return err
		}
	}

	if maxDefinitionLevel > 0 {
		if err := c.v1.definitions.readDataPageV1Level(r, "definition"); err != nil {
			return err
		}
	}

	c.repetitionLevels = &c.v1.repetitions.section
	c.definitionLevels = &c.v1.definitions.section
	return nil
}

func (c *ColumnPages) initDataPageV2(h *format.PageHeader) (err error) {
	repetitionsLength := int64(h.DataPageHeaderV2.RepetitionLevelsByteLength)
	definitionsLength := int64(h.DataPageHeaderV2.DefinitionLevelsByteLength)
	levelsLength := repetitionsLength + definitionsLength
	fileOffset := c.fileOffset()

	if repetitionsLength > 0 {
		c.v2.repetitions.setDataPageV2Section(c.column.file, fileOffset, repetitionsLength)
	} else {
		c.v2.repetitions.reset()
	}

	if definitionsLength > 0 {
		c.v2.definitions.setDataPageV2Section(c.column.file, fileOffset+repetitionsLength, definitionsLength)
	} else {
		c.v2.definitions.reset()
	}

	// Skip the levels, we do this instead of positioning the reader at
	// the beginning of the data so the CRC32 checksum gets computed.
	if levelsLength > 0 {
		remainLength := c.data.N
		c.data.N = levelsLength
		defer func() { c.data.N = remainLength - levelsLength }()
		_, err = io.Copy(io.Discard, &c.data)
	}

	c.repetitionLevels = &c.v2.repetitions.section
	c.definitionLevels = &c.v2.definitions.section
	return err
}

func initCompressedPage(page *compressedPageReader, codec format.CompressionCodec, compressed io.Reader) *compressedPageReader {
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

type dataPageLevelV1 struct {
	section bytes.Reader
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
// to allow the content to be decoded lazily layer on.
//
// In the data header format v2, the repetition and definition levels are not
// part of the compressed page data, they can be accessed by slicing a section
// of the file according to the level lengths stored in the column metadata
// header, therefore there is no need to buffer the levels.
func (lvl *dataPageLevelV1) readDataPageV1Level(r io.Reader, typ string) error {
	const defaultLevelBufferSize = 256

	if cap(lvl.data) < 4 {
		lvl.data = make([]byte, 4, defaultLevelBufferSize)
	} else {
		lvl.data = lvl.data[:4]
	}
	if _, err := io.ReadFull(r, lvl.data); err != nil {
		return fmt.Errorf("reading RLE encoded length of %s levels: %w", typ, err)
	}

	n := int(binary.LittleEndian.Uint32(lvl.data))
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

func (lvl *dataPageLevelV2) setDataPageV2Section(file *File, dataPageOffset, dataPageLength int64) {
	lvl.section = *io.NewSectionReader(file, dataPageOffset, dataPageLength)
}

type offsetReader struct {
	reader *bufio.Reader
	offset int64
}

func (r *offsetReader) ReadByte() (byte, error) {
	b, err := r.reader.ReadByte()
	if err == nil {
		r.offset++
	}
	return b, err
}

func (r *offsetReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.offset += int64(n)
	return n, err
}

func (r *offsetReader) WriteTo(w io.Writer) (int64, error) {
	n, err := r.reader.WriteTo(w)
	r.offset += n
	return n, err
}

type emptyReader struct{}

func (emptyReader) ReadByte() (byte, error)          { return 0, io.EOF }
func (emptyReader) Read([]byte) (int, error)         { return 0, io.EOF }
func (emptyReader) WriteTo(io.Writer) (int64, error) { return 0, nil }

var (
	_ io.ByteReader = emptyReader{}
	_ io.Reader     = emptyReader{}
	_ io.WriterTo   = emptyReader{}

	_ io.ByteReader = (*offsetReader)(nil)
	_ io.Reader     = (*offsetReader)(nil)
	_ io.WriterTo   = (*offsetReader)(nil)
)
