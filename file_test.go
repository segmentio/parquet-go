package parquet_test

import (
	"io"
	"os"
	"strings"
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

			b := new(strings.Builder)
			parquet.Print(b, "File", p.Root())
			t.Log(b)

			printColumns(t, p.Root(), "")
		})
	}
}

func printColumns(t *testing.T, col *parquet.Column, indent string) {
	t.Logf("%s%s", indent, col)
	indent += ". "

	const bufferSize = 1024
	chunks := col.Chunks()

	for chunks.Next() {
		pages := chunks.Pages()
		dictionary := (parquet.Dictionary)(nil)

		for pages.Next() {
			switch header := pages.PageHeader().(type) {
			case parquet.DictionaryPageHeader:
				dictionaryPage := header.Encoding().NewDecoder(pages.PageData())
				dictionary = col.Type().NewDictionary(bufferSize)

				if err := dictionary.ReadFrom(dictionaryPage); err != nil {
					t.Fatal(err)
				}

			case parquet.DataPageHeader:
				var pageReader parquet.PageReader
				var pageData = header.Encoding().NewDecoder(pages.PageData())

				if dictionary != nil {
					pageReader = parquet.NewIndexedPageReader(pageData, bufferSize, dictionary)
				} else {
					pageReader = col.Type().NewPageReader(pageData, bufferSize)
				}

				dataPageReader := parquet.NewDataPageReader(
					header.RepetitionLevelEncoding().NewDecoder(pages.RepetitionLevels()),
					header.DefinitionLevelEncoding().NewDecoder(pages.DefinitionLevels()),
					header.NumValues(),
					pageReader,
					col.MaxRepetitionLevel(),
					col.MaxDefinitionLevel(),
					bufferSize,
				)

				for {
					_, err := dataPageReader.ReadValue()
					if err != nil {
						if err != io.EOF {
							t.Error(err)
						}
						break
					}
				}
			}

			if err := pages.Err(); err != nil {
				t.Error(err)
			}
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
