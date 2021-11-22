package parquet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

var (
	// Corrupted is an error returned by the Err method of ColumnPages instances
	// when they encountered a mismatch between the CRC checksum recorded in a
	// page header and the one computed while reading the page data.
	Corrupted = errors.New("corrupted")
)

type ColumnPages struct {
	column   *Column
	metadata *format.ColumnMetaData
	header   *format.PageHeader
	reader   dataPageReader
	crc32    crc32Reader
	protocol thrift.CompactProtocol
	decoder  thrift.Decoder
	buffer   [4]byte

	data io.LimitedReader
	page *compressedPageReader

	repetitions encoding.Decoder
	definitions encoding.Decoder
	values      encoding.Decoder

	v1 struct {
		repetitions dataPageLevelV1
		definitions dataPageLevelV1
	}

	v2 struct {
		repetitions dataPageLevelV2
		definitions dataPageLevelV2
	}

	lastPage struct {
		headerChecksum uint32
		readerChecksum uint32
	}

	err error
}

func (c *ColumnPages) Close() error {
	c.header = nil

	if c.repetitions != nil {
		c.repetitions.Close()
		c.repetitions = nil
	}

	if c.definitions != nil {
		c.definitions.Close()
		c.definitions = nil
	}

	if c.values != nil {
		c.values.Close()
		c.values = nil
	}

	if c.page != nil {
		releaseCompressedPageReader(c.page)
		c.page = nil
	}

	switch c.err {
	case nil, io.EOF:
		return nil
	default:
		return c.err
	}
}

func (c *ColumnPages) Err() error {
	if c.err != nil {
		return c.err
	}
	if c.lastPage.headerChecksum != c.lastPage.readerChecksum {
		return fmt.Errorf("crc32 checksum mismatch: 0x%08X != 0x%08X: %w", c.lastPage.headerChecksum, c.lastPage.readerChecksum, Corrupted)
	}
	return nil
}

func (c *ColumnPages) Next() bool {
	if c.err != nil {
		return false
	}

	if c.values != nil {
		c.values.Close()
		c.values = nil
	}

	if c.data.N > 0 {
		if _, err := io.Copy(ioutil.Discard, &c.data); err != nil {
			c.err = fmt.Errorf("skipping unread page data: %w", err)
			return false
		}
	}

	if c.crc32.reader != nil {
		c.lastPage.headerChecksum = uint32(c.header.CRC)
		c.lastPage.readerChecksum = c.crc32.Sum32()
		c.crc32.Reset(nil)
	}

	if c.reader.reader == nil {
		dataPageOffset := c.metadata.DataPageOffset
		section := io.NewSectionReader(c.column.file, dataPageOffset, c.metadata.TotalCompressedSize)
		c.reader.reader = bufio.NewReaderSize(section, defaultBufferSize)
		c.reader.offset = dataPageOffset
		c.decoder.Reset(c.protocol.NewReader(&c.reader))
	}

	header := new(format.PageHeader)
	if err := c.decoder.Decode(header); err != nil {
		if err != io.EOF {
			err = fmt.Errorf("decoding page header: %w", err)
		}
		c.err = err
		return false
	}

	reader := io.Reader(&c.reader)
	if header.CRC != 0 {
		c.crc32.Reset(reader)
		reader = &c.crc32
	}

	c.header = header
	c.data.R = reader
	c.data.N = int64(header.CompressedPageSize)
	return true
}

func (c *ColumnPages) Header() *format.PageHeader {
	return c.header
}

func (c *ColumnPages) NumValues() int {
	if h := c.header; h != nil {
		switch h.Type {
		case format.DataPage:
			return int(h.DataPageHeader.NumValues)
		case format.DataPageV2:
			return int(h.DataPageHeaderV2.NumValues)
		}
	}
	return 0
}

func (c *ColumnPages) Statistics() *format.Statistics {
	if h := c.header; h != nil {
		switch h.Type {
		case format.DataPage:
			return &h.DataPageHeader.Statistics
		case format.DataPageV2:
			return &h.DataPageHeaderV2.Statistics
		}
	}
	return nil
}

func (c *ColumnPages) DecodeBoolean(repetitions, definitions []int8, values []bool) (int, error) {
	return c.decode(Boolean, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeBoolean(values[:d.numValues])
		if err != nil {
			return err
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], false
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeInt32(repetitions, definitions []int8, values []int32) (int, error) {
	return c.decode(Int32, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeInt32(values[:d.numValues])
		if err != nil {
			return err
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], 0
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeInt64(repetitions, definitions []int8, values []int64) (int, error) {
	return c.decode(Int64, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeInt64(values[:d.numValues])
		if err != nil {
			return err
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], 0
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeInt96(repetitions, definitions []int8, values [][12]byte) (int, error) {
	return c.decode(Int96, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeInt96(values[:d.numValues])
		if err != nil {
			return err
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], [12]byte{}
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeFloat(repetitions, definitions []int8, values []float32) (int, error) {
	return c.decode(Float, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeFloat(values[:d.numValues])
		if err != nil {
			return err
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], 0
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeDouble(repetitions, definitions []int8, values []float64) (int, error) {
	return c.decode(Double, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeDouble(values[:d.numValues])
		if err != nil {
			return err
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], 0
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeByteArray(repetitions, definitions []int8, values [][]byte) (int, error) {
	return c.decode(ByteArray, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeByteArray(values[:d.numValues])
		if err != nil {
			return err
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], nil
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeFixedLenByteArray(repetitions, definitions []int8, values []byte) (int, error) {
	return c.decode(FixedLenByteArray, repetitions, definitions, func(d decoding) error {
		typeLength := schemaElementType{c.column.schema}.Length()
		_, err := c.values.DecodeFixedLenByteArray(typeLength, values[:d.numValues])
		if err != nil {
			return err
		}
		if len(d.definitions) != d.numValues {
			size := typeLength
			zero := make([]byte, size)
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				vi := values[i*size : i*size+size]
				vj := values[j*size : j*size+size]
				copy(vi, vj)
				copy(vj, zero)
			})
		}
		return nil
	})
}

type decoding struct {
	repetitions        []int8
	definitions        []int8
	numValues          int
	maxDefinitionLevel int8
	maxRepetitionLevel int8
}

func (c *ColumnPages) decode(valueType Kind, repetitions, definitions []int8, decode func(decoding) error) (int, error) {
	if columnType := (schemaElementType{c.column.schema}).Kind(); columnType != valueType {
		return 0, fmt.Errorf("cannot decode %s column into values of type %s", columnType, valueType)
	}

	switch {
	case len(repetitions) < len(definitions):
		definitions = definitions[:len(repetitions)]
	case len(repetitions) > len(definitions):
		repetitions = repetitions[:len(definitions)]
	}

	if c.values == nil {
		var enc format.Encoding
		var err error

		switch h := c.header; h.Type {
		case format.DataPage:
			c.page = initCompressedPage(c.page, c.metadata.Codec, &c.data)
			enc = h.DataPageHeader.Encoding
			err = c.initDataPageV1(c.page)

		case format.DataPageV2:
			enc = h.DataPageHeaderV2.Encoding
			err = c.initDataPageV2(h)
			if err == nil {
				codec := format.Uncompressed
				if h.DataPageHeaderV2.IsCompressed == nil || *h.DataPageHeaderV2.IsCompressed {
					codec = c.metadata.Codec
				}
				c.page = initCompressedPage(c.page, codec, &c.data)
			}

		default:
			err = fmt.Errorf("cannot decode page of type %s", c.header.Type)
		}

		if err != nil {
			return 0, err
		}

		c.values = initDecoder(c.values, c.page, lookupEncoding(enc))
		c.values.SetBitWidth(schemaElementType{c.column.schema}.Length())
	}

	depth := c.column.depth
	maxRepetitionLevel := c.column.maxRepetitionLevel
	maxDefinitionLevel := c.column.maxDefinitionLevel
	numValues := 0

	if maxRepetitionLevel == 0 {
		for i := range repetitions {
			repetitions[i] = 0
		}
		numValues = len(repetitions)
	} else if n, err := c.decodeLevels(c.repetitions, repetitions, "repetition"); err != nil {
		return 0, err
	} else {
		repetitions = repetitions[:n]
		definitions = definitions[:n]
		numValues = n
	}

	if maxDefinitionLevel == 0 {
		for i := range definitions {
			definitions[i] = depth
		}
		numValues = len(definitions)
	} else if n, err := c.decodeLevels(c.definitions, definitions, "definition"); err != nil {
		return 0, err
	} else {
		repetitions = repetitions[:n]
		definitions = definitions[:n]
		numValues = 0

		for _, def := range definitions {
			if def == depth {
				numValues++
			}
		}
	}

	err := decode(decoding{
		repetitions:        repetitions,
		definitions:        definitions,
		numValues:          numValues,
		maxDefinitionLevel: maxDefinitionLevel,
		maxRepetitionLevel: maxRepetitionLevel,
	})
	if err != nil {
		err = fmt.Errorf("decoding %s values from %s column %s: %w",
			valueType,
			strings.ToLower(c.column.schema.RepetitionType.String()),
			strings.Join(c.metadata.PathInSchema, "."),
			dontExpectEOF(err))
	}
	return len(definitions), err
}

func (c *ColumnPages) decodeLevels(dec encoding.Decoder, levels []int8, typ string) (int, error) {
	switch n, err := dec.DecodeInt8(levels); err {
	case nil, io.EOF:
		if n != len(levels) {
			return n, fmt.Errorf("not enough value were read from the %s levels; expected %d but only %d were decoded", typ, len(levels), n)
		}
		return n, nil
	default:
		return n, fmt.Errorf("decoding %s levels: %w", typ, err)
	}
}

func decodeNulls(definitions []int8, numValues int, maxDefinitionLevel int8, move func(int, int)) {
	for i, j := len(definitions)-1, numValues-1; i >= 0 && j >= 0; i-- {
		if definitions[i] == maxDefinitionLevel {
			move(i, j)
			j--
		}
	}
}

func dontExpectEOF(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}

func (c *ColumnPages) initDataPageV1(r io.Reader) error {
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
	c.repetitions = initDecoder(c.repetitions, &c.v1.repetitions, lookupEncoding(h.RepetitionLevelEncoding))
	c.definitions = initDecoder(c.definitions, &c.v1.definitions, lookupEncoding(h.DefinitionLevelEncoding))
	c.repetitions.SetBitWidth(bits.Len8(maxRepetitionLevel))
	c.definitions.SetBitWidth(bits.Len8(maxDefinitionLevel))
	return nil
}

func (c *ColumnPages) initDataPageV2(h *format.PageHeader) error {
	repetitionsLength := int64(h.DataPageHeaderV2.RepetitionLevelsByteLength)
	definitionsLength := int64(h.DataPageHeaderV2.DefinitionLevelsByteLength)

	if repetitionsLength > 0 {
		c.v2.repetitions.setDataPageV2Section(c.column.file, c.reader.offset, repetitionsLength)
		c.repetitions = initDecoder(c.repetitions, &c.v2.repetitions, RLE.LevelEncoding())
		c.repetitions.SetBitWidth(bits.Len8(c.column.maxRepetitionLevel))
	}

	if definitionsLength > 0 {
		c.v2.definitions.setDataPageV2Section(c.column.file, c.reader.offset+repetitionsLength, definitionsLength)
		c.definitions = initDecoder(c.definitions, &c.v2.definitions, RLE.LevelEncoding())
		c.definitions.SetBitWidth(bits.Len8(c.column.maxDefinitionLevel))
	}

	// Skip the levels, we do this instead of positioning the reader at
	// the beginning of the data so the CRC32 checksum gets computed.
	remainLength := c.data.N
	levelsLength := repetitionsLength + definitionsLength
	c.data.N = levelsLength
	defer func() { c.data.N = remainLength - levelsLength }()

	_, err := io.Copy(io.Discard, &c.data)
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

func initDecoder(d encoding.Decoder, r io.Reader, e encoding.Encoding) encoding.Decoder {
	if d == nil || d.Encoding() != e.Encoding() {
		d = e.NewDecoder(r)
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

	if rn, err := io.ReadFull(r, lvl.data[4:]); err != nil {
		return fmt.Errorf("reading %d/%d bytes of %s levels: %w", rn, m, typ, err)
	}

	// Write the encoded length back to the front of the buffer os the whole
	// datagram remains valid to RLE decoders.
	binary.LittleEndian.PutUint32(lvl.data[:4], m)
	lvl.Reader.Reset(lvl.data)
	return nil
}

type dataPageLevelV2 struct {
	io.SectionReader
}

func (lvl *dataPageLevelV2) reset() {
	lvl.SectionReader = *io.NewSectionReader(nil, 0, 0)
}

func (lvl *dataPageLevelV2) setDataPageV2Section(file *File, dataPageOffset, dataPageLength int64) {
	lvl.SectionReader = *io.NewSectionReader(file, dataPageOffset, dataPageLength)
}

// This implementation of io.Reader is used to keep track of the current page
// offset. This is useful to be able to create section readers for data page v2,
// which avoid loading the repetition and definition levels in memory.
type dataPageReader struct {
	// This could be an io.Reader but we specialize it since dataPageReader is
	// only used internally with a bufio.Reader.
	reader *bufio.Reader
	offset int64
}

func (r *dataPageReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.offset += int64(n)
	return n, err
}
