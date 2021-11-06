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
			numValues, numNulls := pages.NumValues(), pages.NumNulls()
			repetitions := make([]int32, numValues)
			definitions := make([]int32, numValues)
			//fmt.Println("num values =", numValues)
			//fmt.Println("num nulls  =", numNulls)

			var n int
			var err error
			switch col.Type() {
			case schema.Boolean:
				n, err = pages.Decode(repetitions, definitions, make([]bool, numValues-numNulls))
			case schema.Int32:
				n, err = pages.Decode(repetitions, definitions, make([]int32, numValues-numNulls))
			case schema.Int64:
				n, err = pages.Decode(repetitions, definitions, make([]int64, numValues-numNulls))
			case schema.Int96:
				n, err = pages.Decode(repetitions, definitions, make([][12]byte, numValues-numNulls))
			case schema.Float:
				n, err = pages.Decode(repetitions, definitions, make([]float32, numValues-numNulls))
			case schema.Double:
				n, err = pages.Decode(repetitions, definitions, make([]float64, numValues-numNulls))
			case schema.ByteArray:
				n, err = pages.Decode(repetitions, definitions, make([][]byte, numValues-numNulls))
			case schema.FixedLenByteArray:
				n, err = pages.Decode(repetitions, definitions, make([]byte, col.TypeLength()*(numValues-numNulls)))
			}

			if err != nil {
				t.Fatalf("unexpected error decoding values: %v", err)
			} else if n != numValues {
				t.Fatalf("wrong number of values decoded: want=%d got=%d", numValues, n)
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
