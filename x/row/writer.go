package row

import (
	"io"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/format"
)

type Writer[T any] struct {
	output io.Writer
	buffer []byte

	fileMetaData
	sorting []format.SortingColumn
	numRows int64
}

func NewWriter[T any](output io.Writer, options ...parquet.WriterOption) *Writer {
	config, err := parquet.NewWriterConfig(options...)
	if err != nil {
		panic(err)
	}
	return &Writer[T]{
		output: output,
		buffer: make([]byte, 0, config.WriteBufferSize),
	}
}

func (w *Writer[T]) Reset(output io.Writer) {
	w.output = output
	w.buffer = w.buffer[:0]
}

func (w *Writer[T]) Close() error {

	return nil
}

func (w *Writer[T]) Flush() error {

	return nil
}

func (w *Writer[T]) Write(rows []T) (n int, err error) {
	return
}

func (w *Writer[T]) WriteRows(rows []parquet.Row) (n int, err error) {
	return
}
