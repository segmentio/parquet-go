package parquet

import (
	"io"

	"github.com/segmentio/parquet/deprecated"
	"github.com/segmentio/parquet/encoding"
)

type optionalValueReader struct {
	values             ValueReader
	index              int
	maxDefinitionLevel int8
	definitionLevels   []int8
}

func (r *optionalValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) {
		for n < len(values) && r.index < len(r.definitionLevels) && isNull(r.index, r.maxDefinitionLevel, r.definitionLevels) {
			values[n] = Value{definitionLevel: r.definitionLevels[r.index]}
			r.index++
		}

		i := r.index
		j := n
		for j < len(values) && i < len(r.definitionLevels) && !isNull(i, r.maxDefinitionLevel, r.definitionLevels) {
			i++
			j++
		}

		if j > n {
			for j, err = r.values.ReadValues(values[n:j]); j > 0; j-- {
				values[n].definitionLevel = r.maxDefinitionLevel
				r.index++
				n++
			}
			if err != nil {
				return n, err
			}
		}

		if r.index == len(r.definitionLevels) {
			return n, io.EOF
		}
	}
	return n, nil
}

type repeatedValueReader struct {
	values             ValueReader
	index              int
	maxDefinitionLevel int8
	maxRepetitionLevel int8
	definitionLevels   []int8
	repetitionLevels   []int8
}

func (r *repeatedValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) {
		for n < len(values) && r.index < len(r.definitionLevels) && isNull(r.index, r.maxDefinitionLevel, r.definitionLevels) {
			values[n] = Value{
				repetitionLevel: r.repetitionLevels[r.index],
				definitionLevel: r.definitionLevels[r.index],
			}
			r.index++
		}

		i := r.index
		j := n
		for j < len(values) && i < len(r.definitionLevels) && !isNull(i, r.maxDefinitionLevel, r.definitionLevels) {
			i++
			j++
		}

		if j > n {
			for j, err = r.values.ReadValues(values[n:j]); j > 0; j-- {
				values[n].repetitionLevel = r.maxRepetitionLevel
				values[n].definitionLevel = r.maxDefinitionLevel
				r.index++
				n++
			}
			if err != nil {
				return n, err
			}
		}

		if r.index == len(r.definitionLevels) {
			return n, io.EOF
		}
	}
	return n, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type booleanValueReader struct{ values []bool }

func (r *booleanValueReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueBoolean(r.values[i])
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		return n, io.EOF
	}
	return n, nil
}

type int32ValueReader struct{ values []int32 }

func (r *int32ValueReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueInt32(r.values[i])
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		return n, io.EOF
	}
	return n, nil
}

type int64ValueReader struct{ values []int64 }

func (r *int64ValueReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueInt64(r.values[i])
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		return n, io.EOF
	}
	return n, nil
}

type int96ValueReader struct{ values []deprecated.Int96 }

func (r *int96ValueReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueInt96(r.values[i])
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		return n, io.EOF
	}
	return n, nil
}

type floatValueReader struct{ values []float32 }

func (r *floatValueReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueFloat(r.values[i])
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		return n, io.EOF
	}
	return n, nil
}

type doubleValueReader struct{ values []float64 }

func (r *doubleValueReader) ReadValues(values []Value) (n int, err error) {
	n = min(len(values), len(r.values))
	for i := 0; i < n; i++ {
		values[i] = makeValueDouble(r.values[i])
	}
	if r.values = r.values[n:]; len(r.values) == 0 {
		return n, io.EOF
	}
	return n, nil
}

type byteArrayValueReader struct {
	values encoding.ByteArrayList
	index  int
}

func (r *byteArrayValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && r.index < r.values.Len() {
		values[n] = makeValueBytes(ByteArray, r.values.Index(r.index)).Clone()
		r.index++
		n++
	}
	if r.index == r.values.Len() {
		return n, io.EOF
	}
	return n, nil
}

type fixedLenByteArrayValueReader struct {
	size int
	data []byte
}

func (r *fixedLenByteArrayValueReader) ReadValues(values []Value) (n int, err error) {
	for n < len(values) && len(r.data) >= r.size {
		values[n] = makeValueBytes(FixedLenByteArray, r.data[:r.size])
		r.data = r.data[r.size:]
		n++
	}
	if len(r.data) < r.size {
		return n, io.EOF
	}
	return n, nil
}
