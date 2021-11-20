package stringio

import "io"

func StringWriter(w io.Writer) io.StringWriter {
	sw, _ := w.(io.StringWriter)
	if sw != nil {
		return sw
	}
	return &writer{writer: w}
}

type writer struct {
	writer io.Writer
	buffer []byte
}

func (w *writer) WriteString(s string) (int, error) {
	w.buffer = append(w.buffer[:0], s...)
	return w.writer.Write(w.buffer)
}
