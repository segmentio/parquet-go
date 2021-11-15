package parquet_test

import (
	"fmt"
	"io"
	"os"
	"os/exec"
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
	defer os.Remove(tmp.Name())

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

	schema := parquet.SchemaOf(rows[0])
	writer := parquet.NewWriter(file, schema)

	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatal("writing row:", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal("closing:", err)
	}

	dump(t, tmp.Name())
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

func dump(t *testing.T, path string) {
	parquetTools := exec.Command("parquet-tools", "dump", path)
	parquetTools.Stdin = os.Stdin
	parquetTools.Stdout = os.Stdout
	parquetTools.Stderr = os.Stderr

	if err := parquetTools.Run(); err != nil {
		t.Error(err)
	}
}
