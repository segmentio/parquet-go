package parquet_test

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/segmentio/parquet"
)

func TestWriterSimple(t *testing.T) {
	type RowType struct {
		FirstName string `parquet:"first_name"`
		LastName  string `parquet:"last_name"`
	}

	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tmp.Name())

	file := tmp //new(bytes.Buffer)
	rows := []*RowType{
		&RowType{
			FirstName: "Han",
			LastName:  "Solo",
		},

		&RowType{
			FirstName: "Leia",
			LastName:  "Skywalker",
		},

		&RowType{
			FirstName: "Luke",
			LastName:  "Skywalker",
		},
	}

	schema := parquet.SchemaOf(new(RowType))
	writer := parquet.NewWriter(&debugWriter{writer: file}, schema)

	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatal("writing row:", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal("closing:", err)
	}

	/*
		content := bytes.NewReader(file.Bytes())
		p, err := parquet.OpenFile(content, content.Size())
		if err != nil {
			t.Fatal(err)
		}
		printColumns(t, p.Root(), "")
	*/
}

type debugWriter struct {
	writer io.Writer
	offset int64
}

func (d *debugWriter) Write(b []byte) (int, error) {
	n, err := d.writer.Write(b)
	fmt.Printf("writing %d bytes at offset %d => %d %v\n", len(b), d.offset, n, err)
	d.offset += int64(n)
	return n, err
}
