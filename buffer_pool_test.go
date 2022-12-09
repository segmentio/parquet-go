package parquet_test

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/segmentio/parquet-go"
)

func TestBufferPool(t *testing.T) {
	testBufferPool(t, parquet.NewBufferPool())
}

func TestFileBufferPool(t *testing.T) {
	testBufferPool(t, parquet.NewFileBufferPool("/tmp", "buffers.*"))
}

func testBufferPool(t *testing.T, pool parquet.BufferPool) {
	tests := []struct {
		scenario string
		function func(*testing.T, parquet.BufferPool)
	}{
		{
			scenario: "write bytes",
			function: testBufferPoolWriteBytes,
		},

		{
			scenario: "write string",
			function: testBufferPoolWriteString,
		},

		{
			scenario: "copy to buffer",
			function: testBufferPoolCopyToBuffer,
		},

		{
			scenario: "copy from buffer",
			function: testBufferPoolCopyFromBuffer,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) { test.function(t, pool) })
	}
}

func testBufferPoolWriteBytes(t *testing.T, pool parquet.BufferPool) {
	const content = "Hello World!"

	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	_, err := buffer.Write([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	assertBufferContent(t, buffer, content)
}

func testBufferPoolWriteString(t *testing.T, pool parquet.BufferPool) {
	const content = "Hello World!"

	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	_, err := io.WriteString(buffer, content)
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, buffer, content)
}

func testBufferPoolCopyToBuffer(t *testing.T, pool parquet.BufferPool) {
	const content = "ABC"

	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	reader := strings.NewReader(content)
	_, err := io.Copy(buffer, struct{ io.Reader }{reader})
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, buffer, content)
}

func testBufferPoolCopyFromBuffer(t *testing.T, pool parquet.BufferPool) {
	const content = "0123456789"

	buffer := pool.GetBuffer()
	defer pool.PutBuffer(buffer)

	if _, err := io.WriteString(buffer, content); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	writer := new(bytes.Buffer)
	_, err := io.Copy(struct{ io.Writer }{writer}, buffer)
	if err != nil {
		t.Fatal(err)
	}

	assertBufferContent(t, bytes.NewReader(writer.Bytes()), content)
}

func assertBufferContent(t *testing.T, b io.ReadSeeker, s string) {
	t.Helper()

	offset, err := b.Seek(0, io.SeekStart)
	if err != nil {
		t.Error("seek:", err)
	}
	if offset != 0 {
		t.Errorf("seek: invalid offset returned: want=0 got=%d", offset)
	}
	if err := iotest.TestReader(b, []byte(s)); err != nil {
		t.Error("iotest:", err)
	}
}
