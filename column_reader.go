package parquet

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/internal/bits"
)

// ColumnReader is an interface implemented by types which support reading
// columns of values. The interface extends ValueReader to work on top of
// parquet encodings.
//
// Implementations of ColumnReader may also provide extensions that the
// application can detect using type assertions. For example, readers for
// columns of INT32 values may implement the parquet.Int32Reader interface
// as a mechanism to provide a type safe and more efficient access to the
// column values.
type ColumnReader interface {
	ValueReader

	// Returns the type of values read.
	Type() Type

	// Returns the column number of values read.
	Column() int

	// Resets the reader state to read values from the given decoder.
	//
	// Column readers created from parquet types are initialized to an empty
	// state and will return io.EOF on every read until a decoder is installed
	// via a call to Reset.
	Reset(decoder encoding.Decoder)
}

type columnReader struct {
	remain             int
	numValues          int
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	repetitions        levelReader
	definitions        levelReader
	values             ColumnReader
}

func newColumnReader(values ColumnReader, maxRepetitionLevel, maxDefinitionLevel int8, bufferSize int) *columnReader {
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

	return &columnReader{
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		repetitions:        makeLevelReader(repetitionBufferSize),
		definitions:        makeLevelReader(definitionBufferSize),
		values:             values,
	}
}

func (r *columnReader) Type() Type { return r.values.Type() }

func (r *columnReader) Column() int { return r.values.Column() }

func (r *columnReader) ReadValues(values []Value) (int, error) {
	if r.values == nil {
		return 0, io.EOF
	}
	read := 0
	columnIndex := ^int16(r.Column())

	for r.remain > 0 && len(values) > 0 {
		var err error
		var repetitionLevels []int8
		var definitionLevels []int8
		var numValues = r.remain

		if len(values) < numValues {
			numValues = len(values)
		}

		if r.maxRepetitionLevel > 0 {
			repetitionLevels, err = r.repetitions.peekLevels()
			if err != nil {
				return read, fmt.Errorf("decoding repetition level from data page of column %d: %w", r.Column(), err)
			}
			if len(repetitionLevels) < numValues {
				numValues = len(repetitionLevels)
			}
		}

		if r.maxDefinitionLevel > 0 {
			definitionLevels, err = r.definitions.peekLevels()
			if err != nil {
				return read, fmt.Errorf("decoding definition level from data page of column %d: %w", r.Column(), err)
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
		numNulls := countLevelsNotEqual(definitionLevels, r.maxDefinitionLevel)
		wantRead := numValues - numNulls
		n, err := r.values.ReadValues(values[:wantRead])
		if n < wantRead && err != nil {
			if err == io.EOF {
				// EOF should not happen at this stage since we successfully
				// decoded levels.
				err = fmt.Errorf("after reading %d/%d values: %w", r.numValues-r.remain, r.numValues, io.ErrUnexpectedEOF)
			}
			return read, fmt.Errorf("decoding values from data page of column %d: %w", r.Column(), err)
		}

		for i, j := n-1, len(definitionLevels)-1; j >= 0; j-- {
			if definitionLevels[j] != r.maxDefinitionLevel {
				values[j] = Value{columnIndex: columnIndex}
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

		values = values[numValues:]
		r.repetitions.discardLevels(len(repetitionLevels))
		r.definitions.discardLevels(len(definitionLevels))
		r.remain -= numValues
		read += numValues
	}

	if r.remain == 0 {
		return read, io.EOF
	}

	return read, nil
}

func (r *columnReader) reset(numValues int, repetitions, definitions, values encoding.Decoder) {
	if repetitions != nil {
		repetitions.SetBitWidth(bits.Len8(r.maxRepetitionLevel))
	}
	if definitions != nil {
		definitions.SetBitWidth(bits.Len8(r.maxDefinitionLevel))
	}
	r.remain = numValues
	r.numValues = numValues
	r.repetitions.reset(repetitions)
	r.definitions.reset(definitions)
	r.values.Reset(values)
}

func (r *columnReader) Reset(encoding.Decoder) {
	panic("BUG: parquet.columnReader.Reset must not be called")
}

type levelReader struct {
	decoder encoding.Decoder
	levels  []int8
	offset  int
	count   int
}

func makeLevelReader(bufferSize int) levelReader {
	return levelReader{
		levels: make([]int8, 0, bufferSize),
	}
}

func (r *levelReader) readLevel() (int8, error) {
	for {
		if r.offset < len(r.levels) {
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
	if r.offset == len(r.levels) {
		if err := r.decodeLevels(); err != nil {
			return nil, err
		}
	}
	return r.levels[r.offset:], nil
}

func (r *levelReader) discardLevels(n int) {
	remain := len(r.levels) - r.offset
	switch {
	case n > remain:
		panic("BUG: cannot discard more levels than buffered")
	case n == remain:
		r.levels = r.levels[:0]
		r.offset = 0
	default:
		r.offset += n
	}
}

func (r *levelReader) decodeLevels() error {
	n, err := r.decoder.DecodeInt8(r.levels[:cap(r.levels)])
	if n == 0 {
		return err
	}
	r.levels = r.levels[:n]
	r.offset = 0
	r.count += n
	return nil
}

func (r *levelReader) reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.levels = r.levels[:0]
	r.offset = 0
	r.count = 0
}

// The types below are implementations of the ColumnReader interface for each
// primitive type supported by parquet.
//
// The readers use an in-memory intermediary buffer to support decoding arrays
// of values from the underlying decoder, which are then boxed into the []Value
// buffer passed to ReadValues. When the program converts type checks the
// readers for more specific interfaces (e.g. parquet.Int32Reader), the values
// can be decoded directly from the underlying decoder, there is no need for
// the intermediary buffers so they are lazily allocated only if the ReadValues
// methods are called.

type booleanColumnReader struct {
	typ         Type
	decoder     encoding.Decoder
	buffer      []bool
	offset      int
	bufferSize  int
	columnIndex int16
}

func newBooleanColumnReader(typ Type, columnIndex int16, bufferSize int) *booleanColumnReader {
	return &booleanColumnReader{
		typ:         typ,
		bufferSize:  bufferSize,
		columnIndex: ^columnIndex,
	}
}

func (r *booleanColumnReader) Type() Type { return r.typ }

func (r *booleanColumnReader) Column() int { return int(^r.columnIndex) }

func (r *booleanColumnReader) ReadBooleans(values []bool) (n int, err error) {
	if r.offset < len(r.buffer) {
		n = copy(values, r.buffer[r.offset:])
		r.offset += n
		values = values[n:]
	}
	if r.decoder == nil {
		err = io.EOF
	} else {
		var d int
		d, err = r.decoder.DecodeBoolean(values)
		n += d
	}
	return n, err
}

func (r *booleanColumnReader) ReadValues(values []Value) (n int, err error) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]bool, 0, atLeastOne(r.bufferSize))
	}

	for {
		for r.offset < len(r.buffer) && n < len(values) {
			values[n] = makeValueBoolean(r.buffer[r.offset])
			values[n].columnIndex = r.columnIndex
			r.offset++
			n++
		}

		if n == len(values) {
			return n, nil
		}
		if r.decoder == nil {
			return n, io.EOF
		}

		buffer := r.buffer[:cap(r.buffer)]
		d, err := r.decoder.DecodeBoolean(buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d]
		r.offset = 0
	}
}

func (r *booleanColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer = r.buffer[:0]
	r.offset = 0
}

type int32ColumnReader struct {
	typ         Type
	decoder     encoding.Decoder
	buffer      []int32
	offset      int
	bufferSize  int
	columnIndex int16
}

func newInt32ColumnReader(typ Type, columnIndex int16, bufferSize int) *int32ColumnReader {
	return &int32ColumnReader{
		typ:         typ,
		bufferSize:  bufferSize,
		columnIndex: ^columnIndex,
	}
}

func (r *int32ColumnReader) Type() Type { return r.typ }

func (r *int32ColumnReader) Column() int { return int(^r.columnIndex) }

func (r *int32ColumnReader) ReadInt32s(values []int32) (n int, err error) {
	if r.offset < len(r.buffer) {
		n = copy(values, r.buffer[r.offset:])
		r.offset += n
		values = values[n:]
	}
	if r.decoder == nil {
		err = io.EOF
	} else {
		var d int
		d, err = r.decoder.DecodeInt32(values)
		n += d
	}
	return n, err
}

func (r *int32ColumnReader) ReadValues(values []Value) (n int, err error) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]int32, 0, atLeastOne(r.bufferSize))
	}

	for {
		for r.offset < len(r.buffer) && n < len(values) {
			values[n] = makeValueInt32(r.buffer[r.offset])
			values[n].columnIndex = r.columnIndex
			r.offset++
			n++
		}

		if n == len(values) {
			return n, nil
		}
		if r.decoder == nil {
			return n, io.EOF
		}

		buffer := r.buffer[:cap(r.buffer)]
		d, err := r.decoder.DecodeInt32(buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d]
		r.offset = 0
	}
}

func (r *int32ColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer = r.buffer[:0]
	r.offset = 0
}

type int64ColumnReader struct {
	typ         Type
	decoder     encoding.Decoder
	buffer      []int64
	offset      int
	bufferSize  int
	columnIndex int16
}

func newInt64ColumnReader(typ Type, columnIndex int16, bufferSize int) *int64ColumnReader {
	return &int64ColumnReader{
		typ:         typ,
		bufferSize:  bufferSize,
		columnIndex: ^columnIndex,
	}
}

func (r *int64ColumnReader) Type() Type { return r.typ }

func (r *int64ColumnReader) Column() int { return int(^r.columnIndex) }

func (r *int64ColumnReader) ReadInt64s(values []int64) (n int, err error) {
	if r.offset < len(r.buffer) {
		n = copy(values, r.buffer[r.offset:])
		r.offset += n
		values = values[n:]
	}
	if r.decoder == nil {
		err = io.EOF
	} else {
		var d int
		d, err = r.decoder.DecodeInt64(values)
		n += d
	}
	return n, err
}

func (r *int64ColumnReader) ReadValues(values []Value) (n int, err error) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]int64, 0, atLeastOne(r.bufferSize))
	}

	for {
		for r.offset < len(r.buffer) && n < len(values) {
			values[n] = makeValueInt64(r.buffer[r.offset])
			values[n].columnIndex = r.columnIndex
			r.offset++
			n++
		}

		if n == len(values) {
			return n, nil
		}
		if r.decoder == nil {
			return n, io.EOF
		}

		buffer := r.buffer[:cap(r.buffer)]
		d, err := r.decoder.DecodeInt64(buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d]
		r.offset = 0
	}
}

func (r *int64ColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer = r.buffer[:0]
	r.offset = 0
}

type int96ColumnReader struct {
	typ         Type
	decoder     encoding.Decoder
	buffer      []deprecated.Int96
	offset      int
	bufferSize  int
	columnIndex int16
}

func newInt96ColumnReader(typ Type, columnIndex int16, bufferSize int) *int96ColumnReader {
	return &int96ColumnReader{
		typ:         typ,
		bufferSize:  bufferSize,
		columnIndex: ^columnIndex,
	}
}

func (r *int96ColumnReader) Type() Type { return r.typ }

func (r *int96ColumnReader) Column() int { return int(^r.columnIndex) }

func (r *int96ColumnReader) ReadInt96s(values []deprecated.Int96) (n int, err error) {
	if r.offset < len(r.buffer) {
		n = copy(values, r.buffer[r.offset:])
		r.offset += n
		values = values[n:]
	}
	if r.decoder == nil {
		err = io.EOF
	} else {
		var d int
		d, err = r.decoder.DecodeInt96(values)
		n += d
	}
	return n, err
}

func (r *int96ColumnReader) ReadValues(values []Value) (n int, err error) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]deprecated.Int96, 0, atLeastOne(r.bufferSize))
	}

	for {
		for r.offset < len(r.buffer) && n < len(values) {
			values[n] = makeValueInt96(r.buffer[r.offset])
			values[n].columnIndex = r.columnIndex
			r.offset++
			n++
		}

		if n == len(values) {
			return n, nil
		}
		if r.decoder == nil {
			return n, io.EOF
		}

		buffer := r.buffer[:cap(r.buffer)]
		d, err := r.decoder.DecodeInt96(buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d]
		r.offset = 0
	}
}

func (r *int96ColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer = r.buffer[:0]
	r.offset = 0
}

type floatColumnReader struct {
	typ         Type
	decoder     encoding.Decoder
	buffer      []float32
	offset      int
	bufferSize  int
	columnIndex int16
}

func newFloatColumnReader(typ Type, columnIndex int16, bufferSize int) *floatColumnReader {
	return &floatColumnReader{
		typ:         typ,
		bufferSize:  bufferSize,
		columnIndex: ^columnIndex,
	}
}

func (r *floatColumnReader) Type() Type { return r.typ }

func (r *floatColumnReader) Column() int { return int(^r.columnIndex) }

func (r *floatColumnReader) ReadFloats(values []float32) (n int, err error) {
	if r.offset < len(r.buffer) {
		n = copy(values, r.buffer[r.offset:])
		r.offset += n
		values = values[n:]
	}
	if r.decoder == nil {
		err = io.EOF
	} else {
		var d int
		d, err = r.decoder.DecodeFloat(values)
		n += d
	}
	return n, err
}

func (r *floatColumnReader) ReadValues(values []Value) (n int, err error) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]float32, 0, atLeastOne(r.bufferSize))
	}

	for {
		for r.offset < len(r.buffer) && n < len(values) {
			values[n] = makeValueFloat(r.buffer[r.offset])
			values[n].columnIndex = r.columnIndex
			r.offset++
			n++
		}

		if n == len(values) {
			return n, nil
		}
		if r.decoder == nil {
			return n, io.EOF
		}

		buffer := r.buffer[:cap(r.buffer)]
		d, err := r.decoder.DecodeFloat(buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d]
		r.offset = 0
	}
}

func (r *floatColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer = r.buffer[:0]
	r.offset = 0
}

type doubleColumnReader struct {
	typ         Type
	decoder     encoding.Decoder
	buffer      []float64
	offset      int
	bufferSize  int
	columnIndex int16
}

func newDoubleColumnReader(typ Type, columnIndex int16, bufferSize int) *doubleColumnReader {
	return &doubleColumnReader{
		typ:         typ,
		bufferSize:  bufferSize,
		columnIndex: ^columnIndex,
	}
}

func (r *doubleColumnReader) Type() Type { return r.typ }

func (r *doubleColumnReader) Column() int { return int(^r.columnIndex) }

func (r *doubleColumnReader) ReadDoubles(values []float64) (n int, err error) {
	if r.offset < len(r.buffer) {
		n = copy(values, r.buffer[r.offset:])
		r.offset += n
		values = values[n:]
	}
	if r.decoder == nil {
		err = io.EOF
	} else {
		var d int
		d, err = r.decoder.DecodeDouble(values)
		n += d
	}
	return n, err
}

func (r *doubleColumnReader) ReadValues(values []Value) (n int, err error) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]float64, 0, atLeastOne(r.bufferSize))
	}

	for {
		for r.offset < len(r.buffer) && n < len(values) {
			values[n] = makeValueDouble(r.buffer[r.offset])
			values[n].columnIndex = r.columnIndex
			r.offset++
			n++
		}

		if n == len(values) {
			return n, nil
		}
		if r.decoder == nil {
			return n, io.EOF
		}

		buffer := r.buffer[:cap(r.buffer)]
		d, err := r.decoder.DecodeDouble(buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d]
		r.offset = 0
	}
}

func (r *doubleColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer = r.buffer[:0]
	r.offset = 0
}

type byteArrayColumnReader struct {
	typ         Type
	decoder     encoding.Decoder
	buffer      encoding.ByteArrayList
	offset      int
	columnIndex int16
}

func newByteArrayColumnReader(typ Type, columnIndex int16, bufferSize int) *byteArrayColumnReader {
	return &byteArrayColumnReader{
		typ:         typ,
		buffer:      encoding.MakeByteArrayList(atLeastOne(bufferSize / 16)),
		columnIndex: ^columnIndex,
	}
}

func (r *byteArrayColumnReader) Type() Type { return r.typ }

func (r *byteArrayColumnReader) Column() int { return int(^r.columnIndex) }

func (r *byteArrayColumnReader) readByteArrays(do func([]byte) bool) (n int, err error) {
	for {
		for r.offset < r.buffer.Len() {
			if !do(r.buffer.Index(r.offset)) {
				return n, nil
			}
			r.offset++
			n++
		}

		if r.decoder == nil {
			return n, io.EOF
		}

		r.buffer.Reset()
		r.offset = 0

		d, err := r.decoder.DecodeByteArray(&r.buffer)
		if d == 0 {
			return n, err
		}
	}
}

func (r *byteArrayColumnReader) ReadByteArrays(values []byte) (int, error) {
	i := 0
	n, err := r.readByteArrays(func(b []byte) bool {
		k := plain.ByteArrayLengthSize + len(b)
		if k > (len(values) - i) {
			return false
		}
		plain.PutByteArrayLength(values[i:], len(b))
		copy(values[i+plain.ByteArrayLengthSize:], b)
		i += k
		return true
	})
	if i == 0 && err == nil {
		err = io.ErrShortBuffer
	}
	return n, err
}

func (r *byteArrayColumnReader) ReadValues(values []Value) (int, error) {
	i := 0
	return r.readByteArrays(func(b []byte) (ok bool) {
		if ok = i < len(values); ok {
			values[i] = makeValueBytes(ByteArray, copyBytes(b))
			values[i].columnIndex = r.columnIndex
			i++
		}
		return ok
	})
}

func (r *byteArrayColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer.Reset()
	r.offset = 0
}

type fixedLenByteArrayColumnReader struct {
	typ         Type
	decoder     encoding.Decoder
	buffer      []byte
	offset      int
	size        int
	bufferSize  int
	columnIndex int16
}

func newFixedLenByteArrayColumnReader(typ Type, columnIndex int16, bufferSize int) *fixedLenByteArrayColumnReader {
	return &fixedLenByteArrayColumnReader{
		typ:         typ,
		size:        typ.Length(),
		bufferSize:  bufferSize,
		columnIndex: ^columnIndex,
	}
}

func (r *fixedLenByteArrayColumnReader) Type() Type { return r.typ }

func (r *fixedLenByteArrayColumnReader) Column() int { return int(^r.columnIndex) }

func (r *fixedLenByteArrayColumnReader) ReadFixedLenByteArrays(values []byte) (n int, err error) {
	if (len(values) % r.size) != 0 {
		return 0, fmt.Errorf("cannot read FIXED_LEN_BYTE_ARRAY values of size %d into buffer of size %d", r.size, len(values))
	}
	if r.offset < len(r.buffer) {
		i := copy(values, r.buffer[r.offset:])
		n += i / r.size
		r.offset += n
		values = values[i:]
	}
	if r.decoder == nil {
		err = io.EOF
	} else {
		var d int
		d, err = r.decoder.DecodeFixedLenByteArray(r.size, values)
		n += d
	}
	return n, err
}

func (r *fixedLenByteArrayColumnReader) ReadValues(values []Value) (n int, err error) {
	if cap(r.buffer) == 0 {
		r.buffer = make([]byte, 0, atLeast((r.bufferSize/r.size)*r.size, r.size))
	}

	for {
		for (r.offset+r.size) <= len(r.buffer) && n < len(values) {
			values[n] = makeValueBytes(FixedLenByteArray, copyBytes(r.buffer[r.offset:r.offset+r.size]))
			values[n].columnIndex = r.columnIndex
			r.offset += r.size
			n++
		}

		if n == len(values) {
			return n, nil
		}
		if r.decoder == nil {
			return n, io.EOF
		}

		buffer := r.buffer[:cap(r.buffer)]
		d, err := r.decoder.DecodeFixedLenByteArray(r.size, buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d*r.size]
		r.offset = 0
	}
}

func (r *fixedLenByteArrayColumnReader) Reset(decoder encoding.Decoder) {
	r.decoder = decoder
	r.buffer = r.buffer[:0]
	r.offset = 0
}

var (
	_ BooleanReader           = (*booleanColumnReader)(nil)
	_ Int32Reader             = (*int32ColumnReader)(nil)
	_ Int64Reader             = (*int64ColumnReader)(nil)
	_ Int96Reader             = (*int96ColumnReader)(nil)
	_ FloatReader             = (*floatColumnReader)(nil)
	_ DoubleReader            = (*doubleColumnReader)(nil)
	_ ByteArrayReader         = (*byteArrayColumnReader)(nil)
	_ FixedLenByteArrayReader = (*fixedLenByteArrayColumnReader)(nil)
)
