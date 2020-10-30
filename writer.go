package parquet

import (
	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
)

// Encoder describes how a given Go type should be encoded to Parquet.
//
// For a given type, it is reusable and concurrently accessible.
type Encoder struct {
	schema *Schema
}

type EncoderOptions struct {
}

func NewEncoder(v interface{}, opts EncoderOptions) *Encoder {
	// Reuse the struct planner, which is probably ill chosen at the moment.
	structPlanner := StructPlannerOf(v)
	schema := structPlanner.Plan().schema()
	return &Encoder{
		schema: schema,
	}
}

// To creates or sets the file to write to
func (e *Encoder) To(path string) *Writer {
	file, err := NewFile(path, e.schema)
	if err != nil {
		return nil
	}

	return &Writer{
		encoder: e,
		file:    file,
	}
}

// Writer uses an Encoder to write to a specific location.
// It is stateful and cannot be concurrently accessed.
type Writer struct {
	encoder *Encoder

	// state
	once bool

	file File
}

func (w *Writer) Write(v interface{}) error {
	err := w.init()
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) Close() error {
	return nil
}

func (w *Writer) init() error {
	if w.once {
		return nil
	}

	// construct metadata and write metadata
	md := pthrift.NewFileMetaData()
	md.Version = 0
	md.NumRows = 0

	return nil
}
