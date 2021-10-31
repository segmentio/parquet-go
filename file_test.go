package parquet_test

import (
	"os"
	"testing"

	"github.com/segmentio/parquet"
)

var fixtureFiles = [...]string{
	"fixtures/file.parquet",
	"fixtures/small.parquet",
	"fixtures/trace.snappy.parquet",
}

func TestOpenFile(t *testing.T) {
	for _, path := range fixtureFiles {
		t.Run(path, func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			s, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}

			p, err := parquet.OpenFile(f, s.Size())
			if err != nil {
				t.Fatal(err)
			}

			if size := p.Size(); size != s.Size() {
				t.Errorf("file size mismatch: want=%d got=%d", s.Size(), size)
			}

			printColumns(t, p.Root(), "")
		})
	}
}

func printColumns(t *testing.T, root *parquet.Column, indent string) {
	t.Logf("%s%s", indent, root)
	indent += ". "

	for _, child := range root.Columns() {
		printColumns(t, child, indent)
	}
}
