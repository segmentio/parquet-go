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
	"strings"

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

	repetitions encoding.Decoder
	definitions encoding.Decoder
	values      encoding.Decoder

	v1 struct {
		repetitions dataPageLevelV1
		definitions dataPageLevelV1
	}

	err error
}

func (c *ColumnPages) Close() error {
	c.header = nil

	closeDecoderIfNotNil(c.repetitions)
	closeDecoderIfNotNil(c.definitions)
	closeDecoderIfNotNil(c.values)
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
	closeDecoderIfNotNil(c.values)
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

func (c *ColumnPages) NumNulls() int {
	if h := c.header; h != nil {
		switch h.Type {
		case schema.DataPage:
			return 0 // TODO
		case schema.DataPageV2:
			return int(h.DataPageHeaderV2.NumNulls)
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

		switch c.header.Type {
		case schema.DataPage:
			if c.page == nil {
				c.page = acquireCompressedPageReader(c.metadata.Codec, &c.data)
			} else {
				c.page.Reset(&c.data)
			}
			enc = c.header.DataPageHeader.Encoding
			err = c.resetDataPageV1(&debugReader{c.page})
		case schema.DataPageV2:
			panic("data page v2 not implemented")
		default:
			err = fmt.Errorf("cannot decode page of type %s", c.header.Type)
		}
		if err != nil {
			return 0, err
		}

		c.values = lookupEncoding(enc).NewDecoder(&debugReader{c.page})
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

func closeDecoderIfNotNil(d encoding.Decoder) {
	if d != nil {
		d.Close()
	}
}

func errorDecodingValues(err error, typ schema.Type) error {
	return fmt.Errorf("decoding %s values: %w", typ, err)
}

func dontExpectEOF(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}

type debugReader struct{ r io.Reader }

func (d *debugReader) Read(b []byte) (int, error) {
	n, err := d.r.Read(b)
	//fmt.Printf("Read(%d) => (%d, %v) % 2x\n", len(b), n, err, b[:n])
	return n, err
}
