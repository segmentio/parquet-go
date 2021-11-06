package parquet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/bits"
	"strings"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/schema"
)

var (
	// Corrupted is an error returned by the Err method of ColumnPages instances
	// when they encountered a mismatch between the CRC checksum recorded in a
	// page header and the one computed while reading the page data.
	Corrupted = errors.New("corrupted")
)

type ColumnPages struct {
	column   *Column
	metadata *schema.ColumnMetaData
	header   *schema.PageHeader
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

	if c.crc32.hash != nil {
		c.lastPage.headerChecksum = uint32(c.header.CRC)
		c.lastPage.readerChecksum = c.crc32.hash.Sum32()
		c.crc32.hash.Reset()
	}

	if c.reader.reader == nil {
		dataPageOffset := c.metadata.DataPageOffset
		section := io.NewSectionReader(c.column.file, dataPageOffset, c.metadata.TotalCompressedSize)
		c.reader.reader = bufio.NewReaderSize(section, defaultBufferSize)
		c.reader.offset = dataPageOffset
		c.decoder.Reset(c.protocol.NewReader(&c.reader))
	}

	header := new(schema.PageHeader)
	if err := c.decoder.Decode(header); err != nil {
		if err != io.EOF {
			err = fmt.Errorf("decoding page header: %w", err)
		}
		c.err = err
		return false
	}

	reader := io.Reader(&c.reader)
	if header.CRC != 0 {
		if c.crc32.hash == nil {
			c.crc32.reader = reader
			c.crc32.hash = crc32.NewIEEE()
		}
		reader = &c.crc32
	}

	c.header = header
	c.data.R = reader
	c.data.N = int64(header.CompressedPageSize)
	return true
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

func (c *ColumnPages) DecodeBoolean(repetitions, definitions []int32, values []bool) (int, error) {
	return c.decode(schema.Boolean, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeBoolean(values[:d.numValues])
		if err != nil {
			return dontExpectEOF(err)
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], false
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeInt32(repetitions, definitions []int32, values []int32) (int, error) {
	return c.decode(schema.Int32, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeInt32(values[:d.numValues])
		if err != nil {
			return dontExpectEOF(err)
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], 0
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeInt64(repetitions, definitions []int32, values []int64) (int, error) {
	return c.decode(schema.Int64, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeInt64(values[:d.numValues])
		if err != nil {
			return dontExpectEOF(err)
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], 0
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeInt96(repetitions, definitions []int32, values [][12]byte) (int, error) {
	return c.decode(schema.Int96, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeInt96(values[:d.numValues])
		if err != nil {
			return dontExpectEOF(err)
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], [12]byte{}
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeFloat(repetitions, definitions []int32, values []float32) (int, error) {
	return c.decode(schema.Float, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeFloat(values[:d.numValues])
		if err != nil {
			return dontExpectEOF(err)
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], 0
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeDouble(repetitions, definitions []int32, values []float64) (int, error) {
	return c.decode(schema.Double, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeDouble(values[:d.numValues])
		if err != nil {
			return dontExpectEOF(err)
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], 0
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeByteArray(repetitions, definitions []int32, values [][]byte) (int, error) {
	return c.decode(schema.ByteArray, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeByteArray(values[:d.numValues])
		if err != nil {
			return dontExpectEOF(err)
		}
		if len(d.definitions) != d.numValues {
			decodeNulls(d.definitions, d.numValues, d.maxDefinitionLevel, func(i, j int) {
				values[i], values[j] = values[j], nil
			})
		}
		return nil
	})
}

func (c *ColumnPages) DecodeFixedLenByteArray(repetitions, definitions []int32, values []byte) (int, error) {
	return c.decode(schema.FixedLenByteArray, repetitions, definitions, func(d decoding) error {
		_, err := c.values.DecodeFixedLenByteArray(c.column.TypeLength(), values[:d.numValues])
		if err != nil {
			return dontExpectEOF(err)
		}
		if len(d.definitions) != d.numValues {
			size := c.column.TypeLength()
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
	repetitions        []int32
	definitions        []int32
	numValues          int
	maxDefinitionLevel int
	maxRepetitionLevel int
}

func (c *ColumnPages) decode(valueType schema.Type, repetitions, definitions []int32, decode func(decoding) error) (int, error) {
	if columnType := c.column.Type(); columnType != valueType {
		return 0, fmt.Errorf("cannot decode %s column into values of type %s", columnType, valueType)
	}

	switch {
	case len(repetitions) < len(definitions):
		definitions = definitions[:len(repetitions)]
	case len(repetitions) > len(definitions):
		repetitions = repetitions[:len(definitions)]
	}

	if c.values == nil {
		var enc schema.Encoding
		var err error

		switch h := c.header; h.Type {
		case schema.DataPage:
			if c.page == nil {
				c.page = acquireCompressedPageReader(c.metadata.Codec, &c.data)
			} else {
				c.page.Reset(&c.data)
			}
			enc = h.DataPageHeader.Encoding
			err = c.resetDataPageV1(c.page)

		case schema.DataPageV2:
			enc = schema.RLE
			repetitionsLength := int64(h.DataPageHeaderV2.RepetitionLevelsByteLength)
			definitionsLength := int64(h.DataPageHeaderV2.DefinitionLevelsByteLength)
			c.v2.repetitions.setDataPageV2Section(c.column.file, c.reader.offset, repetitionsLength)
			c.v2.definitions.setDataPageV2Section(c.column.file, c.reader.offset+repetitionsLength, definitionsLength)
			// Skip the levels, we do this instead of positioning the reader at
			// the beginning of the data so the CRC32 gets computed.
			remainLength := c.data.N
			levelsLength := repetitionsLength + definitionsLength
			c.data.N = levelsLength
			_, err = io.Copy(io.Discard, &c.data)
			c.data.N = remainLength - levelsLength
			if err == nil {
				codec := schema.Uncompressed
				if h.DataPageHeaderV2.IsCompressed == nil || *h.DataPageHeaderV2.IsCompressed {
					codec = c.metadata.Codec
				}
				if c.page == nil {
					c.page = acquireCompressedPageReader(codec, &c.data)
				} else {
					c.page.Reset(&c.data)
				}
			}

		default:
			err = fmt.Errorf("cannot decode page of type %s", c.header.Type)
		}

		if err != nil {
			return 0, err
		}

		c.values = lookupEncoding(enc).NewDecoder(c.page)
		c.values.SetBitWidth(c.column.TypeLength())
	}

	depth := c.column.depth
	maxRepetitionLevel := c.column.MaxRepetitionLevel()
	maxDefinitionLevel := c.column.MaxDefinitionLevel()
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
			err)
	}
	return len(definitions), err
}

func (c *ColumnPages) decodeLevels(dec encoding.Decoder, levels []int32, typ string) (int, error) {
	switch n, err := dec.DecodeInt32(levels); err {
	case nil, io.EOF:
		if n != len(levels) {
			return n, fmt.Errorf("not enough value were read from the %s levels; expected %d but only %d were decoded", typ, len(levels), n)
		}
		return n, nil
	default:
		return n, fmt.Errorf("decoding %s levels: %w", typ, err)
	}
}

func decodeNulls(definitions []int32, numValues, maxDefinitionLevel int, move func(int, int)) {
	for i, j := len(definitions)-1, int(numValues)-1; i >= 0 && j >= 0; i-- {
		if definitions[i] == int32(maxDefinitionLevel) {
			move(i, j)
			j--
		}
	}
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

func resetLevelDecoder(d encoding.Decoder, r io.Reader, encoding schema.Encoding) encoding.Decoder {
	if d == nil {
		d = lookupEncoding(encoding).NewDecoder(r)
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
		return fmt.Errorf("reading %d/%d bytes %s levels: %w", rn, m, typ, err)
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

func dontExpectEOF(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
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

type crc32Reader struct {
	reader io.Reader
	hash   hash.Hash32
}

func (r *crc32Reader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.hash.Write(b[:n])
	return n, err
}
