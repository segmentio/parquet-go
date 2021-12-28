package parquet

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/internal/bits"
)

// PageReader reads values from a data page.
//
// PageReader implements the ValueReader interface; when they exist,
// the reader decodes repetition and definition levels in order to assign
// levels to values returned to the application, which includes producing
// null values when needed.
type PageReader struct {
	remain             int
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	columnIndex        int8
	repetitions        levelReader
	definitions        levelReader
	values             ValueDecoder
}

func NewPageReader(typ Type, maxRepetitionLevel, maxDefinitionLevel, columnIndex int8, bufferSize int) *PageReader {
	bufferSize /= 2
	repetitionBufferSize := 0
	definitionBufferSize := 0

	switch {
	case maxRepetitionLevel > 0 && maxDefinitionLevel > 0:
		repetitionBufferSize = bufferSize / 2
		definitionBufferSize = bufferSize / 2

	case maxRepetitionLevel > 0:
		repetitionBufferSize = bufferSize

	case maxDefinitionLevel > 0:
		definitionBufferSize = bufferSize
	}

	return &PageReader{
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		columnIndex:        ^columnIndex,
		repetitions:        makeLevelReader(repetitionBufferSize),
		definitions:        makeLevelReader(definitionBufferSize),
		values:             typ.NewValueDecoder(bufferSize),
	}
}

func (r *PageReader) Reset(numValues int, repetitions, definitions, values encoding.Decoder) {
	if repetitions != nil {
		repetitions.SetBitWidth(bits.Len8(r.maxRepetitionLevel))
	}
	if definitions != nil {
		definitions.SetBitWidth(bits.Len8(r.maxDefinitionLevel))
	}
	r.remain = numValues
	r.repetitions.reset(repetitions)
	r.definitions.reset(definitions)
	r.values.Reset(values)
}

func (r *PageReader) ReadValues(values []Value) (int, error) {
	read := 0

	for r.remain > 0 && len(values) > 0 {
		var err error
		var repetitionLevels []int8
		var definitionLevels []int8
		var numNulls int
		var numValues = r.remain

		if len(values) < numValues {
			numValues = len(values)
		}

		if r.maxRepetitionLevel > 0 {
			repetitionLevels, err = r.repetitions.peekLevels()
			if err != nil {
				return read, fmt.Errorf("reading parquet repetition level from data page: %w", err)
			}
			if len(repetitionLevels) < numValues {
				numValues = len(repetitionLevels)
			}
		}

		if r.maxDefinitionLevel > 0 {
			definitionLevels, err = r.definitions.peekLevels()
			if err != nil {
				return read, fmt.Errorf("reading parquet definition level from data page: %w", err)
			}
			if len(definitionLevels) < numValues {
				numValues = len(definitionLevels)
			}
		}

		if len(repetitionLevels) > 0 {
			repetitionLevels = repetitionLevels[:numValues]
		}
		if len(definitionLevels) > 0 {
			definitionLevels = definitionLevels[:numValues]
		}
		for _, d := range definitionLevels {
			if d != r.maxDefinitionLevel {
				numNulls++
			}
		}

		if r.values == nil {
			return read, io.EOF
		}

		n, err := r.values.ReadValues(values[:numValues-numNulls])
		if err != nil {
			if err == io.EOF {
				// EOF should not happen at this stage since we successfully
				// decoded levels.
				err = io.ErrUnexpectedEOF
			}
			return read, fmt.Errorf("reading parquet values from data page: %w", err)
		}

		for i, j := n-1, len(definitionLevels)-1; j >= 0; j-- {
			if definitionLevels[j] != r.maxDefinitionLevel {
				values[j] = Value{}
			} else {
				values[j] = values[i]
				i--
			}
		}

		for i, lvl := range repetitionLevels {
			values[i].repetitionLevel = lvl
		}

		for i, lvl := range definitionLevels {
			values[i].definitionLevel = lvl
		}

		for i := range values[:numValues] {
			values[i].columnIndex = r.columnIndex
		}

		values = values[numValues:]
		r.repetitions.discardLevels(len(repetitionLevels))
		r.definitions.discardLevels(len(definitionLevels))
		r.remain -= numValues
		read += numValues
	}

	if r.remain == 0 && read == 0 {
		return 0, io.EOF
	}

	return read, nil
}

type levelReader struct {
	decoder encoding.Decoder
	levels  []int8
	offset  uint
	count   uint
}

func makeLevelReader(bufferSize int) levelReader {
	return levelReader{
		levels: make([]int8, 0, bufferSize),
	}
}

func (r *levelReader) readLevel() (int8, error) {
	for {
		if r.offset < uint(len(r.levels)) {
			lvl := r.levels[r.offset]
			r.offset++
			return lvl, nil
		}
		if err := r.decodeLevels(); err != nil {
			return -1, err
		}
	}
}

func (r *levelReader) peekLevels() ([]int8, error) {
	if r.offset == uint(len(r.levels)) {
		if err := r.decodeLevels(); err != nil {
			return nil, err
		}
	}
	return r.levels[r.offset:], nil
}

func (r *levelReader) discardLevels(n int) {
	remain := uint(len(r.levels)) - r.offset
	switch {
	case uint(n) > remain:
		panic("BUG: cannot discard more levels than buffered")
	case uint(n) == remain:
		r.levels = r.levels[:0]
		r.offset = 0
	default:
		r.offset += uint(n)
	}
}

func (r *levelReader) decodeLevels() error {
	n, err := r.decoder.DecodeInt8(r.levels[:cap(r.levels)])
	if n == 0 {
		return err
	}
	r.levels = r.levels[:n]
	r.offset = 0
	r.count += uint(n)
	return nil
}

func (r *levelReader) reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.levels = r.levels[:0]
	r.offset = 0
	r.count = 0
}

var (
	_ ValueReader = (*PageReader)(nil)
)
