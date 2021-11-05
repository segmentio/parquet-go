package parquet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/bits"
	"reflect"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/encoding"
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

	repetitions encoding.Int32Decoder
	definitions encoding.Int32Decoder
	values      encoding.Decoder

	v1 struct {
		repetitions dataPageLevelV1
		definitions dataPageLevelV1
	}

	err error
}

func (c *ColumnPages) Close() error {
	c.header = nil

	closeIfNotNil(c.repetitions)
	closeIfNotNil(c.definitions)
	closeIfNotNil(c.values)
	c.repetitions = nil
	c.definitions = nil
	c.values = nil

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

func closeIfNotNil(c io.Closer) {
	if c != nil {
		c.Close()
	}
}

func (c *ColumnPages) Next() bool {
	if c.err != nil {
		return false
	}

	if c.data.N > 0 {
		if _, err := io.Copy(ioutil.Discard, &c.data); err != nil {
			c.setError(fmt.Errorf("skipping unread page data: %w", err))
			return false
		}
	}

	if c.reader == nil {
		section := io.NewSectionReader(c.column.file, c.metadata.DataPageOffset, c.metadata.TotalCompressedSize)
		c.reader = bufio.NewReaderSize(section, defaultBufferSize)
		c.decoder.Reset(c.protocol.NewReader(c.reader))
	}

	header := new(schema.PageHeader)
	if err := c.decoder.Decode(header); err != nil {
		c.setError(fmt.Errorf("decoding page header: %w", err))
		return false
	}

	c.header = header
	c.data.R = c.reader
	c.data.N = int64(header.CompressedPageSize)
	closeIfNotNil(c.values)
	c.values = nil
	return true
}

func (c *ColumnPages) setError(err error) {
	c.header, c.err = nil, err
}

func (c *ColumnPages) Header() *schema.PageHeader {
	return c.header
}

func (c *ColumnPages) NumValues() int {
	if h := c.header; h != nil {
		switch h.Type {
		case schema.DataPage:
			return int(h.DataPageHeader.NumValues)
		case schema.DataPageV2:
			return int(h.DataPageHeaderV2.NumValues)
		}
	}
	return 0
}

func (c *ColumnPages) Statistics() *schema.Statistics {
	if h := c.header; h != nil {
		switch h.Type {
		case schema.DataPage:
			return &h.DataPageHeader.Statistics
		case schema.DataPageV2:
			return &h.DataPageHeaderV2.Statistics
		}
	}
	return nil
}

func (c *ColumnPages) Decode(repetitions, definitions []int32, values interface{}) (int, error) {
	typ := c.column.schema.Type

	switch {
	case len(repetitions) < len(definitions):
		definitions = definitions[:len(repetitions)]
	case len(repetitions) > len(definitions):
		repetitions = repetitions[:len(definitions)]
	}

	if c.values == nil {
		var enc schema.Encoding
		var err error

		switch c.header.Type {
		case schema.DataPage:
			if c.page == nil {
				c.page = acquireCompressedPageReader(c.metadata.Codec, &c.data)
			} else {
				c.page.Reset(&c.data)
			}
			enc = c.header.DataPageHeader.Encoding
			err = c.resetDataPageV1(c.page)
		//case schema.DataPageV2:
		// TODO
		default:
			err = fmt.Errorf("cannot decode page of type %s", c.header.Type)
		}
		if err != nil {
			return 0, err
		}

		c.values = newDecoder(c.page, typ, enc)
	}

	switch typ {
	case schema.Boolean:
		if v, ok := values.([]bool); ok {
			return c.decodeBoolean(repetitions, definitions, v)
		}
	case schema.Int32:
		if v, ok := values.([]int32); ok {
			return c.decodeInt32(repetitions, definitions, v)
		}
	case schema.Int64:
		if v, ok := values.([]int64); ok {
			return c.decodeInt64(repetitions, definitions, v)
		}
	case schema.Int96:
		if v, ok := values.([][12]byte); ok {
			return c.decodeInt96(repetitions, definitions, v)
		}
	case schema.Float:
		if v, ok := values.([]float32); ok {
			return c.decodeFloat(repetitions, definitions, v)
		}
	case schema.Double:
		if v, ok := values.([]float64); ok {
			return c.decodeDouble(repetitions, definitions, v)
		}
	case schema.ByteArray:
		if v, ok := values.([][]byte); ok {
			return c.decodeByteArray(repetitions, definitions, v)
		}
	case schema.FixedLenByteArray:
		if v, ok := values.([]byte); ok {
			size := int(c.column.schema.TypeLength)
			return c.decodeFixedLenByteArray(repetitions, definitions, size, v)
		}
	}

	return 0, fmt.Errorf("cannot decode %s into values of type %s", typ, reflect.TypeOf(values))
}

func (c *ColumnPages) decodeRepetitionsAndDefinitions(repetitions, definitions []int32) (int, error) {
	return 0, nil
}

func (c *ColumnPages) decodeBoolean(repetitions, definitions []int32, values []bool) (int, error) {
	if len(repetitions) < len(values) {
		values = values[:len(repetitions)]
	}
	n, err := c.values.(encoding.BooleanDecoder).DecodeBoolean(values)
	if err != nil {
		return n, err
	}
	return c.decodeRepetitionsAndDefinitions(repetitions[:n], definitions[:n])
}

func (c *ColumnPages) decodeInt32(repetitions, definitions []int32, values []int32) (int, error) {
	if len(repetitions) < len(values) {
		values = values[:len(repetitions)]
	}
	n, err := c.values.(encoding.Int32Decoder).DecodeInt32(values)
	if err != nil {
		return n, err
	}
	return c.decodeRepetitionsAndDefinitions(repetitions, definitions)
}

func (c *ColumnPages) decodeInt64(repetitions, definitions []int32, values []int64) (int, error) {
	if len(repetitions) < len(values) {
		values = values[:len(repetitions)]
	}
	n, err := c.values.(encoding.Int64Decoder).DecodeInt64(values)
	if err != nil {
		return n, err
	}
	return c.decodeRepetitionsAndDefinitions(repetitions, definitions)
}

func (c *ColumnPages) decodeInt96(repetitions, definitions []int32, values [][12]byte) (int, error) {
	if len(repetitions) < len(values) {
		values = values[:len(repetitions)]
	}
	n, err := c.values.(encoding.Int96Decoder).DecodeInt96(values)
	if err != nil {
		return n, err
	}
	return c.decodeRepetitionsAndDefinitions(repetitions, definitions)
}

func (c *ColumnPages) decodeFloat(repetitions, definitions []int32, values []float32) (int, error) {
	if len(repetitions) < len(values) {
		values = values[:len(repetitions)]
	}
	n, err := c.values.(encoding.FloatDecoder).DecodeFloat(values)
	if err != nil {
		return n, err
	}
	return c.decodeRepetitionsAndDefinitions(repetitions, definitions)
}

func (c *ColumnPages) decodeDouble(repetitions, definitions []int32, values []float64) (int, error) {
	if len(repetitions) < len(values) {
		values = values[:len(repetitions)]
	}
	n, err := c.values.(encoding.DoubleDecoder).DecodeDouble(values)
	if err != nil {
		return n, err
	}
	return c.decodeRepetitionsAndDefinitions(repetitions, definitions)
}

func (c *ColumnPages) decodeByteArray(repetitions, definitions []int32, values [][]byte) (int, error) {
	if len(repetitions) < len(values) {
		values = values[:len(repetitions)]
	}
	n, err := c.values.(encoding.ByteArrayDecoder).DecodeByteArray(values)
	if err != nil {
		return n, err
	}
	return c.decodeRepetitionsAndDefinitions(repetitions, definitions)
}

func (c *ColumnPages) decodeFixedLenByteArray(repetitions, definitions []int32, size int, values []byte) (int, error) {
	if len(repetitions) < len(values) {
		values = values[:len(repetitions)]
	}
	n, err := c.values.(encoding.FixedLenByteArrayDecoder).DecodeFixedLenByteArray(size, values)
	if err != nil {
		return n, err
	}
	return c.decodeRepetitionsAndDefinitions(repetitions, definitions)
}

func (c *ColumnPages) resetDataPageV1(r io.Reader) error {
	maxRepetitionLevel := c.column.maxRepetitionLevel
	maxDefinitionLevel := c.column.maxDefinitionLevel
	c.v1.repetitions.reset()
	c.v1.repetitions.reset()

	if maxRepetitionLevel > 0 {
		if err := c.v1.repetitions.readDataPageV1Level(r, &c.buffer, "repetition"); err != nil {
			return err
		}
	}

	if maxDefinitionLevel > 0 {
		if err := c.v1.definitions.readDataPageV1Level(r, &c.buffer, "definition"); err != nil {
			return err
		}
	}

	h := c.header.DataPageHeader
	c.repetitions = resetLevelDecoder(c.repetitions, &c.v1.repetitions, h.RepetitionLevelEncoding)
	c.definitions = resetLevelDecoder(c.definitions, &c.v1.definitions, h.DefinitionLevelEncoding)
	c.repetitions.SetBitWidth(bits.Len32(uint32(maxRepetitionLevel)))
	c.definitions.SetBitWidth(bits.Len32(uint32(maxDefinitionLevel)))
	return nil
}

func resetLevelDecoder(d encoding.Int32Decoder, r io.Reader, encoding schema.Encoding) encoding.Int32Decoder {
	if d == nil {
		d = lookupEncoding(encoding).NewInt32Decoder(r)
	} else {
		d.Reset(r)
	}
	return d
}

type dataPageLevelV1 struct {
	bytes.Reader
	data []byte
}

func (lvl *dataPageLevelV1) reset() {
	lvl.Reader.Reset(nil)
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
func (lvl *dataPageLevelV1) readDataPageV1Level(r io.Reader, buf *[4]byte, typ string) error {
	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return fmt.Errorf("reading RLE encoded length of %s levels: %w", typ, err)
	}

	// Work on the assumption that the level is encoded with RLE, in which case
	// the section is prefixed with a 4 byte length of the data.
	m := binary.LittleEndian.Uint32(buf[:4])
	n := int(m) + 4
	if cap(lvl.data) < n {
		lvl.data = make([]byte, n)
	} else {
		lvl.data = lvl.data[:n]
	}

	if rn, err := io.ReadFull(r, lvl.data); err != nil {
		return fmt.Errorf("reading %d/%d bytes %s levels: %w", rn, m, typ, err)
	}

	// Write the encoded length back to the front of the buffer os the whole
	// datagram remains valid to RLE decoders.
	binary.LittleEndian.PutUint32(lvl.data[:4], m)
	lvl.Reader.Reset(lvl.data)
	return nil
}
