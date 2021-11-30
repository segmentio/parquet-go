package debug

import (
	"fmt"
	"io"
)

func Reader(reader io.Reader, prefix string) io.Reader {
	return &ioReader{
		reader: reader,
		prefix: prefix,
	}
}

type ioReader struct {
	reader io.Reader
	prefix string
	offset int64
}

func (d *ioReader) Read(b []byte) (int, error) {
	n, err := d.reader.Read(b)
	fmt.Printf("%s: Read(%d) @%d => %d %v \n  %q\n", d.prefix, len(b), d.offset, n, err, b[:n])
	d.offset += int64(n)
	return n, err
}

func Writer(writer io.Writer, prefix string) io.Writer {
	return &ioWriter{
		writer: writer,
		prefix: prefix,
	}
}

type ioWriter struct {
	writer io.Writer
	prefix string
	offset int64
}

func (d *ioWriter) Write(b []byte) (int, error) {
	n, err := d.writer.Write(b)
	fmt.Printf("%s: Write(%d) @%d => %d %v \n  %q\n", d.prefix, len(b), d.offset, n, err, b[:n])
	d.offset += int64(n)
	return n, err
}
