package parquet

import (
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/internal/bits"
)

type filePageDecoder struct {
	remain             int
	numValues          int
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	columnIndex        int16
	repetitions        levelDecoder
	definitions        levelDecoder
	values             PageValues
}

func newFilePageDecoder(numValues int, columnIndex int16, maxRepetitionLevel, maxDefinitionLevel int8, repetitions, definitions encoding.Decoder, values PageValues) *filePageDecoder {
	if repetitions != nil {
		repetitions.SetBitWidth(bits.Len8(maxRepetitionLevel))
	}
	if definitions != nil {
		definitions.SetBitWidth(bits.Len8(maxDefinitionLevel))
	}
	return &filePageDecoder{
		remain:             numValues,
		numValues:          numValues,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
		columnIndex:        ^columnIndex,
		repetitions:        makeLevelDecoder(repetitions),
		definitions:        makeLevelDecoder(definitions),
		values:             values,
	}
}

func (r *filePageDecoder) Close() error {
	r.remain = 0
	r.repetitions.close()
	r.definitions.close()
	return r.values.Close()
}

func (r *filePageDecoder) ReadValues(values []Value) (int, error) {
	read := 0

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
				return read, fmt.Errorf("decoding repetition level from data page of column %d: %w", ^r.columnIndex, err)
			}
			if len(repetitionLevels) < numValues {
				numValues = len(repetitionLevels)
			}
		}

		if r.maxDefinitionLevel > 0 {
			definitionLevels, err = r.definitions.peekLevels()
			if err != nil {
				return read, fmt.Errorf("decoding definition level from data page of column %d: %w", ^r.columnIndex, err)
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
			return read, fmt.Errorf("read error after decoding %d/%d values from data page of column %d: %w", r.numValues-r.remain, r.numValues, ^r.columnIndex, err)
		}

		for i, j := n-1, len(definitionLevels)-1; j >= 0; j-- {
			if definitionLevels[j] != r.maxDefinitionLevel {
				values[j] = Value{columnIndex: r.columnIndex}
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

type levelDecoder struct {
	decoder encoding.Decoder
	levels  []int8
	offset  int
	count   int
}

func makeLevelDecoder(decoder encoding.Decoder) levelDecoder {
	return levelDecoder{
		decoder: decoder,
		levels:  getLevelBuffer(),
	}
}

func (r *levelDecoder) close() {
	putLevelBuffer(r.levels)
	r.decoder = nil
	r.levels = nil
	r.offset = 0
	r.count = 0
}

func (r *levelDecoder) readLevel() (int8, error) {
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

func (r *levelDecoder) peekLevels() ([]int8, error) {
	if r.offset == len(r.levels) {
		if err := r.decodeLevels(); err != nil {
			return nil, err
		}
	}
	return r.levels[r.offset:], nil
}

func (r *levelDecoder) discardLevels(n int) {
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

func (r *levelDecoder) decodeLevels() error {
	n, err := r.decoder.DecodeInt8(r.levels[:cap(r.levels)])
	if n == 0 {
		return err
	}
	r.levels = r.levels[:n]
	r.offset = 0
	r.count += n
	return nil
}

type byteArrayPageDecoder struct {
	decoder     encoding.Decoder
	buffer      *encoding.ByteArrayList
	offset      int
	remain      int
	columnIndex int16
}

func newByteArrayPageDecoder(typ Type, columnIndex int16, numValues int, decoder encoding.Decoder) *byteArrayPageDecoder {
	return &byteArrayPageDecoder{
		decoder:     decoder,
		buffer:      getByteArrayList(),
		remain:      numValues,
		columnIndex: ^columnIndex,
	}
}

func (r *byteArrayPageDecoder) Close() error {
	putByteArrayList(r.buffer)
	r.decoder = nil
	r.buffer = nil
	r.offset = 0
	r.remain = 0
	return nil
}

func (r *byteArrayPageDecoder) readByteArrays(do func([]byte) bool) (n int, err error) {
	for {
		for r.remain > 0 && r.offset < r.buffer.Len() {
			if !do(r.buffer.Index(r.offset)) {
				return n, nil
			}
			r.offset++
			r.remain--
			n++
		}

		if r.remain == 0 || r.decoder == nil {
			return n, io.EOF
		}

		r.buffer.Reset()
		r.offset = 0

		d, err := r.decoder.DecodeByteArray(r.buffer)
		if d == 0 {
			return n, err
		}
	}
}

func (r *byteArrayPageDecoder) ReadRequired(values []byte) (int, error) {
	return r.ReadByteArrays(values)
}

func (r *byteArrayPageDecoder) ReadByteArrays(values []byte) (int, error) {
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

func (r *byteArrayPageDecoder) ReadValues(values []Value) (int, error) {
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

type fixedLenByteArrayPageDecoder struct {
	decoder     encoding.Decoder
	buffer      []byte
	offset      int
	remain      int
	size        int
	columnIndex int16
}

func newFixedLenByteArrayPageDecoder(typ Type, columnIndex int16, numValues int, decoder encoding.Decoder) *fixedLenByteArrayPageDecoder {
	return &fixedLenByteArrayPageDecoder{
		decoder:     decoder,
		buffer:      getBuffer()[:],
		remain:      numValues,
		size:        typ.Length(),
		columnIndex: ^columnIndex,
	}
}

func (r *fixedLenByteArrayPageDecoder) Close() error {
	putBuffer((*[bufferSize]byte)(r.buffer))
	r.decoder = nil
	r.buffer = nil
	r.offset = 0
	r.remain = 0
	return nil
}

func (r *fixedLenByteArrayPageDecoder) ReadRequired(values []byte) (int, error) {
	return r.ReadFixedLenByteArrays(values)
}

func (r *fixedLenByteArrayPageDecoder) ReadFixedLenByteArrays(values []byte) (n int, err error) {
	if (len(values) % r.size) != 0 {
		return 0, fmt.Errorf("cannot read FIXED_LEN_BYTE_ARRAY values of size %d into buffer of size %d", r.size, len(values))
	}
	if r.offset < len(r.buffer) {
		i := copy(values, r.buffer[r.offset:])
		n = i / r.size
		r.offset += i
		r.remain -= i
		values = values[i:]
	}
	if r.decoder == nil {
		err = io.EOF
	} else {
		var d int
		values = values[:min(r.remain, len(values))]
		d, err = r.decoder.DecodeFixedLenByteArray(r.size, values)
		n += d
		r.remain -= d
		if r.remain == 0 && err == nil {
			err = io.EOF
		}
	}
	return n, err
}

func (r *fixedLenByteArrayPageDecoder) ReadValues(values []Value) (n int, err error) {
	for {
		for (r.offset+r.size) <= len(r.buffer) && n < len(values) {
			values[n] = makeValueBytes(FixedLenByteArray, copyBytes(r.buffer[r.offset:r.offset+r.size]))
			values[n].columnIndex = r.columnIndex
			r.offset += r.size
			r.remain -= r.size
			n++
		}

		if r.remain == 0 || r.decoder == nil {
			return n, io.EOF
		}
		if n == len(values) {
			return n, nil
		}

		length := min(r.remain, cap(r.buffer))
		buffer := r.buffer[:length]
		d, err := r.decoder.DecodeFixedLenByteArray(r.size, buffer)
		if d == 0 {
			return n, err
		}

		r.buffer = buffer[:d*r.size]
		r.offset = 0
	}
}

var (
	byteArrayLists sync.Pool // *encoding.ByteArrayList
)

func getByteArrayList() *encoding.ByteArrayList {
	b, _ := byteArrayLists.Get().(*encoding.ByteArrayList)
	if b != nil {
		b.Reset()
	} else {
		buffer := encoding.MakeByteArrayList(bufferSize / 16)
		b = &buffer
	}
	return b
}

func putByteArrayList(b *encoding.ByteArrayList) {
	if b != nil {
		byteArrayLists.Put(b)
	}
}

func getLevelBuffer() []int8 {
	b := getBuffer()
	return unsafe.Slice((*int8)(unsafe.Pointer(&b[0])), bufferSize)
}

func putLevelBuffer(b []int8) {
	if cap(b) == bufferSize {
		b = b[:cap(b)]
		putBuffer((*[bufferSize]byte)(unsafe.Pointer(&b[0])))
	}
}
