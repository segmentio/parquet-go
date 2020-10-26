package parquet

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

func (e *Encoder) To(path string) *Writer {
	return &Writer{
		encoder: e,
	}
}

// Writer uses an Encoder to write to a specific location.
// It is stateful and cannot be concurrently accessed.
type Writer struct {
	encoder *Encoder

	// state
	once bool
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

	return nil
}
