package stringio_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/segmentio/parquet/internal/stringio"
)

func TestStringWriter(t *testing.T) {
	b := new(bytes.Buffer)
	w := struct{ io.Writer }{b} // mask bytes.(*Buffer).WriteString
	s := stringio.StringWriter(&w)

	if n, err := s.WriteString("Hello World!"); err != nil {
		t.Fatal(err)
	} else if n != 12 {
		t.Errorf("wrong number of bytes written: %d", n)
	}
	if data := b.String(); data != "Hello World!" {
		t.Errorf("wrong data written to the buffer: %q", data)
	}
}
