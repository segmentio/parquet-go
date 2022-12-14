package ioext

import "io"

// OffsetTrackingWriter is a io.Writer wrapper which keeps track of the number
// of bytes that have been written.
type OffsetTrackingWriter struct {
	writer io.Writer
	offset int64
}

func (w *OffsetTrackingWriter) Writer() io.Writer {
	return w.writer
}

func (w *OffsetTrackingWriter) Offset() int64 {
	return w.offset
}

func (w *OffsetTrackingWriter) Reset(writer io.Writer) {
	w.writer = writer
	w.offset = 0
}

func (w *OffsetTrackingWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.offset += int64(n)
	return n, err
}

func (w *OffsetTrackingWriter) WriteString(s string) (int, error) {
	n, err := io.WriteString(w.writer, s)
	w.offset += int64(n)
	return n, err
}

func (w *OffsetTrackingWriter) ReadFrom(r io.Reader) (int64, error) {
	// io.Copy will make use of io.ReaderFrom if w.writer implements it.
	n, err := io.Copy(w.writer, r)
	w.offset += n
	return n, err
}

var (
	_ io.ReaderFrom   = (*OffsetTrackingWriter)(nil)
	_ io.StringWriter = (*OffsetTrackingWriter)(nil)
)
