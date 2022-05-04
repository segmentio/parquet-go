package parquet_test

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/segmentio/parquet-go"
)

var testdataFiles []string

func init() {
	entries, _ := os.ReadDir("testdata")
	for _, e := range entries {
		testdataFiles = append(testdataFiles, filepath.Join("testdata", e.Name()))
	}
}

func TestOpenFile(t *testing.T) {
	for _, path := range testdataFiles {
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

			root := p.Root()
			b := new(strings.Builder)
			parquet.PrintSchema(b, root.Name(), root)
			t.Log(b)

			printColumns(t, p.Root(), "")
		})
	}
}

func printColumns(t *testing.T, col *parquet.Column, indent string) {
	if t.Failed() {
		return
	}

	path := strings.Join(col.Path(), ".")
	if col.Leaf() {
		t.Logf("%s%s %v %v", indent, path, col.Encoding(), col.Compression())
	} else {
		t.Logf("%s%s", indent, path)
	}
	indent += ". "

	buffer := make([]parquet.Value, 42)
	pages := col.Pages()
	for {
		p, err := pages.ReadPage()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}

		values := p.Values()
		numValues := int64(0)
		nullCount := int64(0)

		for {
			n, err := values.ReadValues(buffer)
			for _, v := range buffer[:n] {
				if v.Column() != col.Index() {
					t.Errorf("value read from page of column %d says it belongs to column %d", col.Index(), v.Column())
					return
				}
				if v.IsNull() {
					nullCount++
				}
			}
			numValues += int64(n)
			if err != nil {
				if err != io.EOF {
					t.Error(err)
					return
				}
				break
			}
		}

		if numValues != p.NumValues() {
			t.Errorf("page of column %d declared %d values but %d were read", col.Index(), p.NumValues(), numValues)
			return
		}

		if nullCount != p.NumNulls() {
			t.Errorf("page of column %d declared %d nulls but %d were read", col.Index(), p.NumNulls(), nullCount)
			return
		}
	}

	for _, child := range col.Columns() {
		printColumns(t, child, indent)
	}
}

func TestFileKeyValueMetadata(t *testing.T) {
	type Row struct {
		Name string
	}

	f, err := createParquetFile(
		makeRows([]Row{{Name: "A"}, {Name: "B"}, {Name: "C"}}),
		parquet.KeyValueMetadata("hello", "ignore this one"),
		parquet.KeyValueMetadata("hello", "world"),
		parquet.KeyValueMetadata("answer", "42"),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range [][2]string{
		{"hello", "world"},
		{"answer", "42"},
	} {
		key, value := want[0], want[1]
		if found, ok := f.Lookup(key); !ok || found != value {
			t.Errorf("key/value metadata mismatch: want %q=%q but got %q=%q (found=%t)", key, value, key, found, ok)
		}
	}
}
