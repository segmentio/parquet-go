package parquet_test

import (
	"os"
	"testing"

	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/schema"
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

func printColumns(t *testing.T, col *parquet.Column, indent string) {
	t.Logf("%s%s", indent, col)
	indent += ". "

	chunks := col.Chunks()
	for chunks.Next() {
		pages := chunks.DataPages()
		for pages.Next() {
			p := pages.Header()
			//t.Logf(">> %s", p.Type)

			switch p.Type {
			case schema.DataPage:
				//t.Logf(". >> %v %v %v %v", p.DataPageHeader.NumValues, p.DataPageHeader.Encoding, p.DataPageHeader.DefinitionLevelEncoding, p.DataPageHeader.RepetitionLevelEncoding)
			case schema.IndexPage:
			case schema.DictionaryPage:
			case schema.DataPageV2:
			default:
				t.Fatalf("unsupported page type: %d", p.Type)
			}
		}
		if err := pages.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := chunks.Close(); err != nil {
		t.Fatal(err)
	}

	for _, child := range col.Columns() {
		printColumns(t, child, indent)
	}
}
