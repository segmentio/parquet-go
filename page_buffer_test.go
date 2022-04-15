package parquet_test

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/segmentio/parquet-go"
)

func TestPageBufferPool(t *testing.T) {
	testPageBufferPool(t, parquet.NewPageBufferPool())
}

func TestFileBufferPool(t *testing.T) {
	testPageBufferPool(t, parquet.NewFileBufferPool("/tmp", "buffers.*"))
}

func testPageBufferPool(t *testing.T, pool parquet.PageBufferPool) {
	tests := []struct {
		scenario string
		function func(*testing.T, parquet.PageBufferPool)
	}{
		{
			scenario: "write bytes",
			function: testPageBufferPoolWriteBytes,
		},

		{
			scenario: "write string",
			function: testPageBufferPoolWriteString,
		},

		{
			scenario: "copy to buffer",
			function: testPageBufferPoolCopyToBuffer,
		},

		{
			scenario: "copy from buffer",
			function: testPageBufferPoolCopyFromBuffer,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) { test.function(t, pool) })
	}
}

func testPageBufferPoolWriteBytes(t *testing.T, pool parquet.PageBufferPool) {
	const content = "Hello World!"

	buffer := pool.GetPageBuffer()
	_, err := buffer.Write([]byte(content))
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, buffer, content)
}

func testPageBufferPoolWriteString(t *testing.T, pool parquet.PageBufferPool) {
	const content = "Hello World!"

	buffer := pool.GetPageBuffer()
	_, err := io.WriteString(buffer, content)
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, buffer, content)
}

func testPageBufferPoolCopyToBuffer(t *testing.T, pool parquet.PageBufferPool) {
	const content = "ABC"

	buffer := pool.GetPageBuffer()
	reader := strings.NewReader(content)
	_, err := io.Copy(buffer, struct{ io.Reader }{reader})
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, buffer, content)
}

func testPageBufferPoolCopyFromBuffer(t *testing.T, pool parquet.PageBufferPool) {
	const content = "0123456789"

	buffer := pool.GetPageBuffer()
	if _, err := io.WriteString(buffer, content); err != nil {
		t.Fatal(err)
	}

	writer := new(bytes.Buffer)
	_, err := io.Copy(struct{ io.Writer }{writer}, buffer)
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, writer, content)
}

func assertBufferContent(t *testing.T, b io.Reader, s string) {
	t.Helper()

	if err := iotest.TestReader(b, []byte(s)); err != nil {
		t.Error(err)
	}
}
