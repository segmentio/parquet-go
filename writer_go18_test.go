//go:build go1.18

package parquet_test

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go"
)

func BenchmarkGenericWriter(b *testing.B) {
	benchmarkGenericWriter[benchmarkRowType](b)
	benchmarkGenericWriter[booleanColumn](b)
	benchmarkGenericWriter[int32Column](b)
	benchmarkGenericWriter[int64Column](b)
	benchmarkGenericWriter[floatColumn](b)
	benchmarkGenericWriter[doubleColumn](b)
	benchmarkGenericWriter[byteArrayColumn](b)
	benchmarkGenericWriter[fixedLenByteArrayColumn](b)
	benchmarkGenericWriter[stringColumn](b)
	benchmarkGenericWriter[indexedStringColumn](b)
	benchmarkGenericWriter[uuidColumn](b)
	benchmarkGenericWriter[mapColumn](b)
	benchmarkGenericWriter[decimalColumn](b)
	benchmarkGenericWriter[contact](b)
	benchmarkGenericWriter[paddedBooleanColumn](b)
	benchmarkGenericWriter[optionalInt32Column](b)
	benchmarkGenericWriter[repeatedInt32Column](b)
}

func benchmarkGenericWriter[Row generator[Row]](b *testing.B) {
	var model Row
	b.Run(reflect.TypeOf(model).Name(), func(b *testing.B) {
		prng := rand.New(rand.NewSource(0))
		rows := make([]Row, benchmarkNumRows)
		for i := range rows {
			rows[i] = rows[i].generate(prng)
		}

		b.Run("go1.17", func(b *testing.B) {
			writer := parquet.NewWriter(io.Discard, parquet.SchemaOf(rows[0]))
			i := 0
			benchmarkRowsPerSecond(b, func() int {
				for j := 0; j < benchmarkRowsPerStep; j++ {
					if err := writer.Write(&rows[i]); err != nil {
						b.Fatal(err)
					}
				}

				i += benchmarkRowsPerStep
				i %= benchmarkNumRows

				if i == 0 {
					writer.Close()
					writer.Reset(io.Discard)
				}
				return benchmarkRowsPerStep
			})
		})

		b.Run("go1.18", func(b *testing.B) {
			writer := parquet.NewGenericWriter[Row](io.Discard)
			i := 0
			benchmarkRowsPerSecond(b, func() int {
				n, err := writer.Write(rows[i : i+benchmarkRowsPerStep])
				if err != nil {
					b.Fatal(err)
				}

				i += benchmarkRowsPerStep
				i %= benchmarkNumRows

				if i == 0 {
					writer.Close()
					writer.Reset(io.Discard)
				}
				return n
			})
		})
	})
}

func TestIssue272(t *testing.T) {
	type T2 struct {
		X string `parquet:",dict,optional"`
	}

	type T1 struct {
		TA *T2
		TB *T2
	}

	type T struct {
		T1 *T1
	}

	const nRows = 1

	row := T{
		T1: &T1{
			TA: &T2{
				X: "abc",
			},
		},
	}

	rows := make([]T, nRows)
	for i := range rows {
		rows[i] = row
	}

	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[T](b)

	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f := bytes.NewReader(b.Bytes())
	r := parquet.NewGenericReader[T](f)

	parquetRows := make([]parquet.Row, nRows)
	n, err := r.ReadRows(parquetRows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != nRows {
		t.Fatalf("wrong number of rows read: want=%d got=%d", nRows, n)
	}
	for _, r := range parquetRows {
		if d := r[0].DefinitionLevel(); d != 3 {
			t.Errorf("wrong definition level for column 0: %d", d)
		}
		if d := r[1].DefinitionLevel(); d != 1 {
			t.Errorf("wrong definition level for column 1: %d", d)
		}
	}
}

func TestIssue279(t *testing.T) {
	type T2 struct {
		Id   int    `parquet:",plain,optional"`
		Name string `parquet:",plain,optional"`
	}

	type T1 struct {
		TA []*T2
	}

	type T struct {
		T1 *T1
	}

	const nRows = 1

	row := T{
		T1: &T1{
			TA: []*T2{
				{
					Id:   43,
					Name: "john",
				},
			},
		},
	}

	rows := make([]T, nRows)
	for i := range rows {
		rows[i] = row
	}

	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[T](b)

	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f := bytes.NewReader(b.Bytes())
	r := parquet.NewGenericReader[T](f)

	parquetRows := make([]parquet.Row, nRows)
	n, err := r.ReadRows(parquetRows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != nRows {
		t.Fatalf("wrong number of rows read: want=%d got=%d", nRows, n)
	}
	for _, r := range parquetRows {
		if d := r[0].DefinitionLevel(); d != 3 {
			t.Errorf("wrong definition level for column 0: %d", d)
		}
		if d := r[1].DefinitionLevel(); d != 3 {
			t.Errorf("wrong definition level for column 1: %d", d)
		}
	}
}

func TestIssue302(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			name: "SimpleMap",
			fn: func(t *testing.T) {
				type M map[string]int

				type T struct {
					M M `parquet:","`
				}

				b := new(bytes.Buffer)
				_ = parquet.NewGenericWriter[T](b)

			},
		},

		{
			name: "MapWithValueTag",
			fn: func(t *testing.T) {
				type M map[string]int

				type T struct {
					M M `parquet:"," parquet-value:",zstd"`
				}

				b := new(bytes.Buffer)
				_ = parquet.NewGenericWriter[T](b)

			},
		},

		{
			name: "MapWithOptionalTag",
			fn: func(t *testing.T) {
				type M map[string]int

				type T struct {
					M M `parquet:",optional"`
				}

				b := new(bytes.Buffer)
				w := parquet.NewGenericWriter[T](b)
				expect := []T{
					{
						M: M{
							"Holden": 1,
							"Naomi":  2,
						},
					},
					{
						M: nil,
					},
					{
						M: M{
							"Naomi":  1,
							"Holden": 2,
						},
					},
				}
				_, err := w.Write(expect)
				if err != nil {
					t.Fatal(err)
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}

				bufReader := bytes.NewReader(b.Bytes())
				r := parquet.NewGenericReader[T](bufReader)
				values := make([]T, 3)
				_, err = r.Read(values)
				if !reflect.DeepEqual(expect, values) {
					t.Fatalf("values do not match.\n\texpect: %v\n\tactual: %v", expect, values)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, test.fn)
	}
}

func TestIssue347Writer(t *testing.T) {
	type TestType struct {
		Key int
	}

	b := new(bytes.Buffer)
	// instantiating with concrete type shouldn't panic
	_ = parquet.NewGenericWriter[TestType](b)

	// instantiating with schema and interface type parameter shouldn't panic
	schema := parquet.SchemaOf(TestType{})
	_ = parquet.NewGenericWriter[any](b, schema)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("instantiating generic buffer without schema and with interface " +
				"type parameter should panic")
		}
	}()
	_ = parquet.NewGenericWriter[any](b)
}
