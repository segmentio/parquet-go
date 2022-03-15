//go:build go1.18

package parquet_test

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/internal/cast"
)

func TestPage(t *testing.T) {
	t.Run("BOOLEAN", testPageOf[bool])
	t.Run("INT32", testPageOf[int32])
	t.Run("INT64", testPageOf[int64])
	t.Run("INT96", testPageOf[deprecated.Int96])
	t.Run("FLOAT", testPageOf[float32])
	t.Run("DOUBLE", testPageOf[float64])
	t.Run("BYTE_ARRAY", testPageByteArray)
	t.Run("FIXED_LEN_BYTE_ARRAY", testPageFixedLenByteArray)
}

func randValue[T any](r *rand.Rand) (v T) {
	switch (any)(v).(type) {
	case bool:
		return (any)(r.Float64() < 0.5).(T)
	case int32:
		return (any)(r.Int31()).(T)
	case int64:
		return (any)(r.Int63()).(T)
	case deprecated.Int96:
		return (any)(deprecated.Int96{
			0: r.Uint32(),
			1: r.Uint32(),
			2: r.Uint32(),
		}).(T)
	case float32:
		return (any)(r.Float32()).(T)
	case float64:
		return (any)(r.Float64()).(T)
	default:
		panic("NOT IMPLEMENTED")
	}
}

func testPageOf[T any](t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value T }{})
	r := rand.New(rand.NewSource(0))

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest[T]{
			write: func(w parquet.ValueWriter) ([]T, error) {
				values := []T{
					0: randValue[T](r),
					1: randValue[T](r),
				}
				n, err := w.(io.Writer).Write(cast.SliceToBytes(values))
				return values[:n], err
			},
			read: func(r parquet.ValueReader) ([]T, error) {
				values := make([]T, 2)
				n, err := r.(io.Reader).Read(cast.SliceToBytes(values))
				return values[:n], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest[T]{
			write: func(w parquet.ValueWriter) ([]T, error) {
				values := []T{
					0: randValue[T](r),
					1: randValue[T](r),
				}
				n, err := w.(parquet.RequiredWriter[T]).WriteRequired(values)
				return values[:n], err
			},
			read: func(r parquet.ValueReader) ([]T, error) {
				values := make([]T, 2)
				n, err := r.(parquet.RequiredReader[T]).ReadRequired(values)
				return values[:n], err
			},
		})
	})
}

func testPageByteArray(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value []byte }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest[byte]{
			write: func(w parquet.ValueWriter) ([]byte, error) {
				values := []byte{}
				values = plain.AppendByteArray(values, []byte("A"))
				values = plain.AppendByteArray(values, []byte("B"))
				values = plain.AppendByteArray(values, []byte("C"))
				n, err := w.(io.Writer).Write(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) ([]byte, error) {
				values := make([]byte, 3+3*plain.ByteArrayLengthSize)
				n, err := r.(io.Reader).Read(values)
				return values[:n], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest[byte]{
			write: func(w parquet.ValueWriter) ([]byte, error) {
				values := []byte{}
				values = plain.AppendByteArray(values, []byte("A"))
				values = plain.AppendByteArray(values, []byte("B"))
				values = plain.AppendByteArray(values, []byte("C"))
				_, err := w.(parquet.ByteArrayWriter).WriteByteArrays(values)
				return values, err
			},

			read: func(r parquet.ValueReader) ([]byte, error) {
				values := make([]byte, 3+3*plain.ByteArrayLengthSize)
				n, err := r.(parquet.ByteArrayReader).ReadByteArrays(values)
				return values[:n+n*plain.ByteArrayLengthSize], err
			},
		})
	})
}

func testPageFixedLenByteArray(t *testing.T) {
	schema := parquet.SchemaOf(struct{ Value [3]byte }{})

	t.Run("io", func(t *testing.T) {
		testBufferPage(t, schema, pageTest[byte]{
			write: func(w parquet.ValueWriter) ([]byte, error) {
				values := []byte("123456789")
				n, err := w.(io.Writer).Write(values)
				return values[:n], err
			},

			read: func(r parquet.ValueReader) ([]byte, error) {
				values := make([]byte, 3*3)
				n, err := r.(io.Reader).Read(values)
				return values[:n], err
			},
		})
	})

	t.Run("parquet", func(t *testing.T) {
		testPage(t, schema, pageTest[byte]{
			write: func(w parquet.ValueWriter) ([]byte, error) {
				values := []byte("123456789")
				n, err := w.(parquet.FixedLenByteArrayWriter).WriteFixedLenByteArrays(values)
				return values[:3*n], err
			},

			read: func(r parquet.ValueReader) ([]byte, error) {
				values := make([]byte, 3*3)
				n, err := r.(parquet.FixedLenByteArrayReader).ReadFixedLenByteArrays(values)
				return values[:3*n], err
			},
		})
	})
}

type pageTest[T any] struct {
	write func(parquet.ValueWriter) ([]T, error)
	read  func(parquet.ValueReader) ([]T, error)
}

func testPage[T any](t *testing.T, schema *parquet.Schema, test pageTest[T]) {
	t.Run("buffer", func(t *testing.T) { testBufferPage(t, schema, test) })
	t.Run("file", func(t *testing.T) { testFilePage(t, schema, test) })
}

func testBufferPage[T any](t *testing.T, schema *parquet.Schema, test pageTest[T]) {
	buffer := parquet.NewBuffer(schema)
	column := buffer.Column(0).(parquet.ColumnBuffer)

	w, err := test.write(column)
	if err != nil {
		t.Fatal("writing page values:", err)
	}

	r, err := test.read(column.Page().Values())
	if err != io.EOF {
		t.Errorf("expected io.EOF after reading all values but got %v", err)
	}
	if !reflect.DeepEqual(w, r) {
		t.Errorf("wrong values read from the page: got=%+v want=%+v", r, w)
	}
}

func testFilePage[T any](t *testing.T, schema *parquet.Schema, test pageTest[T]) {
	buffer := parquet.NewBuffer(schema)
	column := buffer.Column(0).(parquet.ColumnBuffer)

	w, err := test.write(column)
	if err != nil {
		t.Fatal("writing page values:", err)
	}

	output := new(bytes.Buffer)
	writer := parquet.NewWriter(output)
	n, err := writer.WriteRowGroup(buffer)
	if err != nil {
		t.Fatal("writing parquet file:", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal("writing parquet file:", err)
	}
	if n != buffer.NumRows() {
		t.Fatalf("number of rows written mismatch: got=%d want=%d", n, buffer.NumRows())
	}

	reader := bytes.NewReader(output.Bytes())
	f, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatal("opening parquet file:", err)
	}

	p, err := f.RowGroup(0).Column(0).Pages().ReadPage()
	if err != nil {
		t.Fatal("reading parquet page:", err)
	}

	values := p.Values()
	r, err := test.read(values)
	if err != io.EOF && err != nil {
		t.Errorf("expected io.EOF after reading all values but got %v", err)
	}
	if !reflect.DeepEqual(w, r) {
		t.Errorf("wrong values read from the page: got=%+v want=%+v", r, w)
	}
	if r, err := test.read(values); reflect.ValueOf(r).Len() != 0 || err != io.EOF {
		t.Errorf("expected no data and io.EOF after reading all values but got %v and %v", r, err)
	}
}

type testStruct struct {
	Value *string
}

func TestOptionalPageTrailingNulls(t *testing.T) {
	schema := parquet.SchemaOf(&testStruct{})
	buffer := parquet.NewBuffer(schema)

	str := "test"
	rows := []testStruct{{
		Value: nil,
	}, {
		Value: &str,
	}, {
		Value: nil,
	}}

	for _, row := range rows {
		if err := buffer.WriteRow(schema.Deconstruct(nil, row)); err != nil {
			t.Fatal("writing row:", err)
		}
	}

	resultRows := []parquet.Row{}
	reader := buffer.Rows()
	for {
		row, err := reader.ReadRow(nil)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal("reading rows:", err)
		}
		resultRows = append(resultRows, row)
	}

	if len(resultRows) != len(rows) {
		t.Errorf("wrong number of rows read: got=%d want=%d", len(resultRows), len(rows))
	}
}

func TestOptionalPagePreserveIndex(t *testing.T) {
	schema := parquet.SchemaOf(&testStruct{})
	buffer := parquet.NewBuffer(schema)

	if err := buffer.WriteRow(schema.Deconstruct(nil, &testStruct{Value: nil})); err != nil {
		t.Fatal("writing row:", err)
	}

	row, err := buffer.Rows().ReadRow(nil)
	if err != nil {
		t.Fatal("reading rows:", err)
	}

	if row[0].Column() != 0 {
		t.Errorf("wrong index: got=%d want=%d", row[0].Column(), 0)
	}
}

func TestRepeatedPageTrailingNulls(t *testing.T) {
	type testStruct struct {
		A []string `parquet:"a"`
	}

	s := parquet.SchemaOf(&testStruct{})

	records := []*testStruct{
		{A: nil},
		{A: []string{"test"}},
		{A: nil},
	}

	buf := parquet.NewBuffer(s)
	for _, rec := range records {
		row := s.Deconstruct(nil, rec)
		err := buf.WriteRow(row)
		if err != nil {
			t.Fatal(err)
		}
	}

	resultRows := []parquet.Row{}
	reader := buf.Rows()
	for {
		row, err := reader.ReadRow(nil)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal("reading rows:", err)
		}
		resultRows = append(resultRows, row)
	}

	if len(resultRows) != len(records) {
		t.Errorf("wrong number of rows read: got=%d want=%d", len(resultRows), len(records))
	}
}
