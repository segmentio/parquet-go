package parquet_test

import (
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

	chunks := col.Chunks()
	for chunks.Next() {
		pages := chunks.DataPages()

		for pages.Next() {
			/*
				numValues := pages.NumValues()
				repetitions := make([]int8, numValues)
				definitions := make([]int8, numValues)

				var n int
				var err error
				var typ = col.Type()
				switch typ.Kind() {
				case parquet.Boolean:
					n, err = pages.DecodeBoolean(repetitions, definitions, make([]bool, numValues))
				case parquet.Int32:
					n, err = pages.DecodeInt32(repetitions, definitions, make([]int32, numValues))
				case parquet.Int64:
					n, err = pages.DecodeInt64(repetitions, definitions, make([]int64, numValues))
				case parquet.Int96:
					n, err = pages.DecodeInt96(repetitions, definitions, make([][12]byte, numValues))
				case parquet.Float:
					n, err = pages.DecodeFloat(repetitions, definitions, make([]float32, numValues))
				case parquet.Double:
					n, err = pages.DecodeDouble(repetitions, definitions, make([]float64, numValues))
				case parquet.ByteArray:
					n, err = pages.DecodeByteArray(repetitions, definitions, make([][]byte, numValues))
				case parquet.FixedLenByteArray:
					n, err = pages.DecodeFixedLenByteArray(repetitions, definitions, make([]byte, typ.Length()*numValues))
				}

				if err != nil {
					t.Fatalf("unexpected error decoding values: %v", err)
				} else if n != numValues {
					t.Fatalf("wrong number of values decoded: want=%d got=%d", numValues, n)
				}
			*/

			if err := pages.Err(); err != nil {
				t.Error(err)
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
