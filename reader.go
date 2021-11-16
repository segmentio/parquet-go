package parquet

import (
	"fmt"
	"io"
)

type Reader struct {
}

type columnChunkReader struct {
	chunks *ColumnChunks
	pages  *ColumnPages
	reader pageReader
}

func newColumnChunkReader(column *Column) *columnChunkReader {
	return &columnChunkReader{
		chunks: column.Chunks(),
		reader: newPageReader(column.Type(), nil, 64*1024),
	}
}

func (ccr *columnChunkReader) Close() error {
	if ccr.pages != nil {
		ccr.pages.Close()
	}
	ccr.pages = nil
	ccr.reader = nil
	return ccr.chunks.Close()
}

func (ccr *columnChunkReader) ReadValue() (Value, error) {
	for {
		if ccr.reader != nil {
			v, err := ccr.reader.ReadValue()
			if err == nil || err != io.EOF {
				return v, err
			}
		}

		if ccr.pages != nil {
			if ccr.pages.Next() {
				ccr.reader.Reset(ccr.pages)
				continue
			}
			if err := ccr.pages.Close(); err != nil {
				return Value{}, err
			}
			ccr.pages = nil
		}

		if !ccr.chunks.Next() {
			err := ccr.chunks.Close()
			if err == nil {
				err = io.EOF
			}
			return Value{}, err
		}

		ccr.pages = ccr.chunks.DataPages()
		ccr.reader.Reset(ccr.pages)
	}
}

type pageReader interface {
	ValueReader

	Reset(*ColumnPages)
}

func newPageReader(t Type, pages *ColumnPages, bufferSize int) pageReader {
	switch t.Kind() {
	case Boolean:
		return newBooleanPageReader(pages, bufferSize)
	default:
		return newUnsupportedPageReader(t)
	}
}

type booleanPageReader struct {
	pages       *ColumnPages
	offset      int
	values      []bool
	repetitions []int32
	definitions []int32
}

func newBooleanPageReader(pages *ColumnPages, bufferSize int) *booleanPageReader {
	n := bufferSize / (1 + 4 + 4)
	return &booleanPageReader{
		pages:       pages,
		values:      make([]bool, 0, n),
		repetitions: make([]int32, n),
		definitions: make([]int32, n),
	}
}

func (r *booleanPageReader) Reset(pages *ColumnPages) {
	r.pages = pages
	r.values = r.values[:0]
	r.offset = 0
}

func (r *booleanPageReader) ReadValue() (Value, error) {
	for {
		if i := r.offset; i >= 0 && i < len(r.values) {
			r.offset++
			value := makeValueBoolean(Boolean, r.values[i])
			value.SetRepetitionLevel(r.repetitions[i])
			value.SetDefinitionLevel(r.definitions[i])
			return value, nil
		}

		values := r.values[:cap(r.values)]
		n, err := r.pages.DecodeBoolean(r.repetitions, r.definitions, values)
		if err != nil && n == 0 {
			return Value{}, err
		}

		r.offset = 0
		r.values = r.values[:n]
	}
}

type unsupportedPageReader struct{ typ Type }

func newUnsupportedPageReader(t Type) *unsupportedPageReader {
	return &unsupportedPageReader{typ: t}
}

func (r *unsupportedPageReader) Reset(*ColumnPages) {}
func (r *unsupportedPageReader) ReadValue() (Value, error) {
	return Value{}, fmt.Errorf("cannot read parquet column page of type %s", r.typ.Kind())
}
