package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/internal/quick"
)

func rowsOf(numRows int, model interface{}) rows {
	prng := rand.New(rand.NewSource(0))
	return randomRowsOf(prng, numRows, model)
}

func randomRowsOf(prng *rand.Rand, numRows int, model interface{}) rows {
	typ := reflect.TypeOf(model)
	rows := make(rows, numRows)
	makeValue := quick.MakeValueFuncOf(typ)
	for i := range rows {
		v := reflect.New(typ).Elem()
		makeValue(v, prng)
		rows[i] = v.Interface()
	}
	return rows
}

var readerTests = []struct {
	scenario string
	model    interface{}
}{
	{
		scenario: "BOOLEAN",
		model:    booleanColumn{},
	},

	{
		scenario: "INT32",
		model:    int32Column{},
	},

	{
		scenario: "INT64",
		model:    int64Column{},
	},

	{
		scenario: "INT96",
		model:    int96Column{},
	},

	{
		scenario: "FLOAT",
		model:    floatColumn{},
	},

	{
		scenario: "DOUBLE",
		model:    doubleColumn{},
	},

	{
		scenario: "BYTE_ARRAY",
		model:    byteArrayColumn{},
	},

	{
		scenario: "FIXED_LEN_BYTE_ARRAY",
		model:    fixedLenByteArrayColumn{},
	},

	{
		scenario: "STRING",
		model:    stringColumn{},
	},

	{
		scenario: "STRING (dict)",
		model:    indexedStringColumn{},
	},

	{
		scenario: "UUID",
		model:    uuidColumn{},
	},

	{
		scenario: "DECIMAL",
		model:    decimalColumn{},
	},

	{
		scenario: "AddressBook",
		model:    addressBook{},
	},

	{
		scenario: "one optional level",
		model:    listColumn2{},
	},

	{
		scenario: "one repeated level",
		model:    listColumn1{},
	},

	{
		scenario: "two repeated levels",
		model:    listColumn0{},
	},

	{
		scenario: "three repeated levels",
		model:    listColumn0{},
	},

	{
		scenario: "nested lists",
		model:    nestedListColumn{},
	},

	{
		scenario: "key-value pairs",
		model: struct {
			KeyValuePairs map[utf8string]utf8string
		}{},
	},

	{
		scenario: "multiple key-value pairs",
		model: struct {
			KeyValuePairs0 map[utf8string]utf8string
			KeyValuePairs1 map[utf8string]utf8string
			KeyValuePairs2 map[utf8string]utf8string
		}{},
	},

	{
		scenario: "repeated key-value pairs",
		model: struct {
			RepeatedKeyValuePairs []map[utf8string]utf8string
		}{},
	},

	{
		scenario: "map of repeated values",
		model: struct {
			MapOfRepeated map[utf8string][]utf8string
		}{},
	},
}

func TestReader(t *testing.T) {
	buf := new(bytes.Buffer)
	file := bytes.NewReader(nil)

	for _, test := range readerTests {
		t.Run(test.scenario, func(t *testing.T) {
			const N = 42

			rowType := reflect.TypeOf(test.model)
			rowPtr := reflect.New(rowType)
			rowZero := reflect.Zero(rowType)
			rowValue := rowPtr.Elem()

			for n := 1; n < N; n++ {
				t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
					defer buf.Reset()
					rows := rowsOf(n, test.model)

					if err := writeParquetFileWithBuffer(buf, rows); err != nil {
						t.Fatal(err)
					}

					file.Reset(buf.Bytes())
					r := parquet.NewReader(file, parquet.SchemaOf(test.model))

					for i, v := range rows {
						if err := r.Read(rowPtr.Interface()); err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(rowValue.Interface(), v) {
							t.Errorf("row mismatch at index %d\nwant = %+v\ngot  = %+v", i, v, rowValue.Interface())
						}
						rowValue.Set(rowZero)
					}

					if err := r.Read(rowPtr.Interface()); err != io.EOF {
						t.Errorf("expected EOF after reading all values but got: %v", err)
					}
				})
			}
		})
	}
}

func BenchmarkReaderReadType(b *testing.B) {
	buf := new(bytes.Buffer)
	file := bytes.NewReader(nil)

	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			defer buf.Reset()
			rows := rowsOf(benchmarkNumRows, test.model)

			if err := writeParquetFile(buf, rows); err != nil {
				b.Fatal(err)
			}
			file.Reset(buf.Bytes())
			f, err := parquet.OpenFile(file, file.Size())
			if err != nil {
				b.Fatal(err)
			}

			rowType := reflect.TypeOf(test.model)
			rowPtr := reflect.New(rowType)
			rowZero := reflect.Zero(rowType)
			rowValue := rowPtr.Elem()

			r := parquet.NewReader(f)
			p := rowPtr.Interface()

			benchmarkRowsPerSecond(b, func() (n int) {
				for i := 0; i < benchmarkRowsPerStep; i++ {
					if err := r.Read(p); err != nil {
						if err == io.EOF {
							r.Reset()
						} else {
							b.Fatal(err)
						}
					}
				}
				rowValue.Set(rowZero)
				return benchmarkRowsPerStep
			})

			b.SetBytes(int64(math.Ceil(benchmarkRowsPerStep * float64(file.Size()) / benchmarkNumRows)))
		})
	}
}

func BenchmarkReaderReadRow(b *testing.B) {
	buf := new(bytes.Buffer)
	file := bytes.NewReader(nil)

	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			defer buf.Reset()
			rows := rowsOf(benchmarkNumRows, test.model)

			if err := writeParquetFile(buf, rows); err != nil {
				b.Fatal(err)
			}
			file.Reset(buf.Bytes())
			f, err := parquet.OpenFile(file, file.Size())
			if err != nil {
				b.Fatal(err)
			}

			r := parquet.NewReader(f)
			rowbuf := make([]parquet.Row, benchmarkRowsPerStep)

			benchmarkRowsPerSecond(b, func() int {
				n, err := r.ReadRows(rowbuf)
				if err != nil {
					if err == io.EOF {
						r.Reset()
					} else {
						b.Fatal(err)
					}
				}
				return n
			})

			b.SetBytes(int64(math.Ceil(benchmarkRowsPerStep * float64(file.Size()) / benchmarkNumRows)))
		})
	}
}

func TestReaderReadSubset(t *testing.T) {
	// In this example we'll write 3 columns to the file - X, Y, and Z, but
	// we'll only read out the X and Y columns. Returns true if all writes
	// and reads were successful, and false otherwise.
	type Point3D struct{ X, Y, Z int64 }
	type Point2D struct{ X, Y int64 }

	err := quickCheck(func(points3D []Point3D) bool {
		if len(points3D) == 0 {
			return true
		}
		buf := new(bytes.Buffer)
		err := writeParquetFile(buf, makeRows(points3D))
		if err != nil {
			t.Error(err)
			return false
		}
		reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
		for i := 0; ; i++ {
			row := Point2D{}
			err := reader.Read(&row)
			if err != nil {
				if err == io.EOF && i == len(points3D) {
					break
				}
				t.Error(err)
				return false
			}
			if row != (Point2D{X: points3D[i].X, Y: points3D[i].Y}) {
				t.Errorf("points mismatch at row index %d: want=%v got=%v", i, points3D[i], row)
				return false
			}
		}
		return true
	})
	if err != nil {
		t.Error(err)
	}
}

func TestReaderSeekToRow(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:",dict"`
	}

	rows := rowsOf(10, rowType{})
	buf := new(bytes.Buffer)
	err := writeParquetFile(buf, rows)
	if err != nil {
		t.Fatal(err)
	}

	reader := parquet.NewReader(bytes.NewReader(buf.Bytes()))
	for i := 0; i < 10; i++ {
		if err := reader.SeekToRow(int64(i)); err != nil {
			t.Fatalf("seek to row %d: %v", i, err)
		}

		row := new(rowType)
		err := reader.Read(row)
		if err != nil {
			t.Fatalf("reading row %d: %v", i, err)
		}

		if *row != rows[i] {
			t.Fatalf("row %d mismatch: got=%+v want=%+v", i, *row, rows[i])
		}
	}
}

func TestSeekToRowNoDict(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:","` // no dictionary encoding
	}

	// write samples to in-memory buffer
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema)
	sample := rowType{
		Name: "foo1",
	}
	// write two rows
	w.Write(sample)
	sample.Name = "foo2"
	w.Write(sample)
	w.Close()

	// create reader
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()))

	// read second row
	r.SeekToRow(1)
	row := new(rowType)
	err := r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	// fmt.Println(&sample, row)
	if *row != sample {
		t.Fatalf("read != write")
	}
}

func TestSeekToRowReadAll(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:",dict"`
	}

	// write samples to in-memory buffer
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema)
	sample := rowType{
		Name: "foo1",
	}
	// write two rows
	w.Write(sample)
	sample.Name = "foo2"
	w.Write(sample)
	w.Close()

	// create reader
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()))

	// read first row
	r.SeekToRow(0)
	row := new(rowType)
	err := r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	// read second row
	r.SeekToRow(1)
	row = new(rowType)
	err = r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	// fmt.Println(&sample, row)
	if *row != sample {
		t.Fatalf("read != write")
	}
}

func TestSeekToRowDictReadSecond(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:",dict"`
	}

	// write samples to in-memory buffer
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema)
	sample := rowType{
		Name: "foo1",
	}
	// write two rows
	w.Write(sample)
	sample.Name = "foo2"
	w.Write(sample)
	w.Close()

	// create reader
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()))

	// read second row
	r.SeekToRow(1)
	row := new(rowType)
	err := r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	// fmt.Println(&sample, row)
	if *row != sample {
		t.Fatalf("read != write")
	}
}

func TestSeekToRowDictReadMultiplePages(t *testing.T) {
	type rowType struct {
		Name utf8string `parquet:",dict"`
	}

	// write samples to in-memory buffer
	buf := new(bytes.Buffer)
	schema := parquet.SchemaOf(new(rowType))
	w := parquet.NewWriter(buf, schema, &parquet.WriterConfig{
		PageBufferSize: 10,
	})
	sample := rowType{
		Name: "foo1",
	}

	// write enough rows to spill over a single page
	for i := 0; i < 10; i++ {
		w.Write(sample)
	}
	sample.Name = "foo2"
	w.Write(sample)
	w.Close()

	// create reader
	r := parquet.NewReader(bytes.NewReader(buf.Bytes()))

	// read 11th row
	r.SeekToRow(10)
	row := new(rowType)
	err := r.Read(row)
	if err != nil {
		t.Fatalf("reading row: %v", err)
	}
	if *row != sample {
		t.Fatalf("read != write")
	}
}
