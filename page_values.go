package parquet

import (
	"io"

	"github.com/segmentio/parquet-go/encoding/plain"
)

type optionalValues struct {
	page   *optionalPage
	values PageValues
	offset int
}

func (r *optionalValues) Close() error {
	r.page = nil
	return r.values.Close()
}

func (r *optionalValues) ReadValues(values []Value) (n int, err error) {
	if r.page == nil {
		return 0, io.EOF
	}
	if r.values == nil {
		r.values = r.page.base.Values()
	}
	maxDefinitionLevel := r.page.maxDefinitionLevel
	columnIndex := ^int16(r.page.Column())

	for n < len(values) && r.offset < len(r.page.definitionLevels) {
		for n < len(values) && r.offset < len(r.page.definitionLevels) && r.page.definitionLevels[r.offset] != maxDefinitionLevel {
			values[n] = Value{
				definitionLevel: r.page.definitionLevels[r.offset],
				columnIndex:     columnIndex,
			}
			r.offset++
			n++
		}

		i := n
		j := r.offset
		for i < len(values) && j < len(r.page.definitionLevels) && r.page.definitionLevels[j] == maxDefinitionLevel {
			i++
			j++
		}

		if n < i {
			for j, err = r.values.ReadValues(values[n:i]); j > 0; j-- {
				values[n].definitionLevel = maxDefinitionLevel
				r.offset++
				n++
			}
			// Do not return on an io.EOF here as we may still have null values to read.
			if err != nil && err != io.EOF {
				return n, err
			}
		}
	}

	if r.offset == len(r.page.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

type repeatedValues struct {
	page   *repeatedPage
	values PageValues
	offset int
}

func (r *repeatedValues) Close() error {
	r.page = nil
	return r.values.Close()
}

func (r *repeatedValues) ReadValues(values []Value) (n int, err error) {
	if r.page == nil {
		return 0, io.EOF
	}
	if r.values == nil {
		r.values = r.page.base.Values()
	}
	maxDefinitionLevel := r.page.maxDefinitionLevel
	columnIndex := ^int16(r.page.Column())

	for n < len(values) && r.offset < len(r.page.definitionLevels) {
		for n < len(values) && r.offset < len(r.page.definitionLevels) && r.page.definitionLevels[r.offset] != maxDefinitionLevel {
			values[n] = Value{
				repetitionLevel: r.page.repetitionLevels[r.offset],
				definitionLevel: r.page.definitionLevels[r.offset],
				columnIndex:     columnIndex,
			}
			r.offset++
			n++
		}

		i := n
		j := r.offset
		for i < len(values) && j < len(r.page.definitionLevels) && r.page.definitionLevels[j] == maxDefinitionLevel {
			i++
			j++
		}

		if n < i {
			for j, err = r.values.ReadValues(values[n:i]); j > 0; j-- {
				values[n].repetitionLevel = r.page.repetitionLevels[r.offset]
				values[n].definitionLevel = maxDefinitionLevel
				r.offset++
				n++
			}
			if err != nil && err != io.EOF {
				return n, err
			}
		}
	}

	if r.offset == len(r.page.definitionLevels) {
		err = io.EOF
	}
	return n, err
}

type byteArrayValues struct {
	page   *byteArrayPage
	offset int
}

func (r *byteArrayValues) Close() error {
	r.page = nil
	return nil
}

func (r *byteArrayValues) Read(b []byte) (int, error) {
	_, n, err := r.readByteArrays(b)
	return n, err
}

func (r *byteArrayValues) ReadRequired(values []byte) (int, error) {
	return r.ReadByteArrays(values)
}

func (r *byteArrayValues) ReadByteArrays(values []byte) (int, error) {
	n, _, err := r.readByteArrays(values)
	return n, err
}

func (r *byteArrayValues) readByteArrays(values []byte) (c, n int, err error) {
	if r.page == nil {
		return 0, 0, io.EOF
	}
	for r.offset < r.page.values.Len() {
		b := r.page.values.Index(r.offset)
		k := plain.ByteArrayLengthSize + len(b)
		if k > (len(values) - n) {
			break
		}
		plain.PutByteArrayLength(values[n:], len(b))
		n += plain.ByteArrayLengthSize
		n += copy(values[n:], b)
		r.offset++
		c++
	}
	if r.offset == r.page.values.Len() {
		err = io.EOF
	} else if n == 0 && len(values) > 0 {
		err = io.ErrShortBuffer
	}
	return c, n, err
}

func (r *byteArrayValues) ReadValues(values []Value) (n int, err error) {
	if r.page == nil {
		return 0, io.EOF
	}
	for n < len(values) && r.offset < r.page.values.Len() {
		values[n] = makeValueBytes(ByteArray, r.page.values.Index(r.offset))
		values[n].columnIndex = r.page.columnIndex
		r.offset++
		n++
	}
	if r.offset == r.page.values.Len() {
		err = io.EOF
	}
	return n, err
}

type fixedLenByteArrayValues struct {
	page   *fixedLenByteArrayPage
	offset int
}

func (r *fixedLenByteArrayValues) Close() error {
	r.page = nil
	return nil
}

func (r *fixedLenByteArrayValues) Read(b []byte) (n int, err error) {
	n, err = r.ReadFixedLenByteArrays(b)
	return n * r.page.size, err
}

func (r *fixedLenByteArrayValues) ReadRequired(values []byte) (int, error) {
	return r.ReadFixedLenByteArrays(values)
}

func (r *fixedLenByteArrayValues) ReadFixedLenByteArrays(values []byte) (n int, err error) {
	if r.page == nil {
		return 0, io.EOF
	}
	n = copy(values, r.page.data[r.offset:]) / r.page.size
	r.offset += n * r.page.size
	if r.offset == len(r.page.data) {
		err = io.EOF
	} else if n == 0 && len(values) > 0 {
		err = io.ErrShortBuffer
	}
	return n, err
}

func (r *fixedLenByteArrayValues) ReadValues(values []Value) (n int, err error) {
	if r.page == nil {
		return 0, io.EOF
	}
	for n < len(values) && r.offset < len(r.page.data) {
		values[n] = makeValueBytes(FixedLenByteArray, r.page.data[r.offset:r.offset+r.page.size])
		values[n].columnIndex = r.page.columnIndex
		r.offset += r.page.size
		n++
	}
	if r.offset == len(r.page.data) {
		err = io.EOF
	}
	return n, err
}

type nullValues struct {
	remain      int
	columnIndex int16
}

func newNullValues(typ Type, columnIndex int16, numValues int) *nullValues {
	return &nullValues{
		remain:      numValues,
		columnIndex: ^columnIndex,
	}
}

func (r *nullValues) Close() error {
	r.remain = 0
	return nil
}

func (r *nullValues) ReadValues(values []Value) (int, error) {
	if len(values) > r.remain {
		values = values[:r.remain]
	}
	for i := range values {
		values[i] = Value{columnIndex: r.columnIndex}
	}
	r.remain -= len(values)
	if r.remain == 0 {
		return 0, io.EOF
	}
	return len(values), nil
}

type errorValues struct{ err error }

func (r *errorValues) Close() error                    { return nil }
func (r *errorValues) ReadValues([]Value) (int, error) { return 0, r.err }
