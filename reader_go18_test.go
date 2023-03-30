//go:build go1.18

package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestGenericReader(t *testing.T) {
	testGenericReader[booleanColumn](t)
	testGenericReader[int32Column](t)
	testGenericReader[int64Column](t)
	testGenericReader[int96Column](t)
	testGenericReader[floatColumn](t)
	testGenericReader[doubleColumn](t)
	testGenericReader[byteArrayColumn](t)
	testGenericReader[fixedLenByteArrayColumn](t)
	testGenericReader[stringColumn](t)
	testGenericReader[indexedStringColumn](t)
	testGenericReader[uuidColumn](t)
	testGenericReader[timeColumn](t)
	testGenericReader[timeInMillisColumn](t)
	testGenericReader[mapColumn](t)
	testGenericReader[decimalColumn](t)
	testGenericReader[addressBook](t)
	testGenericReader[contact](t)
	testGenericReader[listColumn2](t)
	testGenericReader[listColumn1](t)
	testGenericReader[listColumn0](t)
	testGenericReader[nestedListColumn1](t)
	testGenericReader[nestedListColumn](t)
	testGenericReader[*contact](t)
	testGenericReader[paddedBooleanColumn](t)
	testGenericReader[optionalInt32Column](t)
	testGenericReader[repeatedInt32Column](t)
}

func testGenericReader[Row any](t *testing.T) {
	var model Row
	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		err := quickCheck(func(rows []Row) bool {
			if len(rows) == 0 {
				return true // TODO: fix support for parquet files with zero rows
			}
			if err := testGenericReaderRows(rows); err != nil {
				t.Error(err)
				return false
			}
			return true
		})
		if err != nil {
			t.Error(err)
		}
	})
}

func testGenericReaderRows[Row any](rows []Row) error {
	setNullPointers(rows)
	buffer := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](buffer)
	_, err := writer.Write(rows)
	if err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	reader := parquet.NewGenericReader[Row](bytes.NewReader(buffer.Bytes()))
	result := make([]Row, len(rows))
	n, err := reader.Read(result)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(rows) {
		return fmt.Errorf("not enough values were read: want=%d got=%d", len(rows), n)
	}
	if !reflect.DeepEqual(rows, result) {
		return fmt.Errorf("rows mismatch:\nwant: %+v\ngot: %+v", rows, result)
	}
	return nil
}

func TestIssue400(t *testing.T) {
	type B struct {
		Name string
	}
	type A struct {
		B []B `parquet:",optional"`
	}

	b := new(bytes.Buffer)
	w := parquet.NewGenericWriter[A](b)
	expect := []A{
		{
			B: []B{
				{
					// 32 bytes random so we can see in the binary parquet if we
					// actually wrote the value
					Name: "9e7eb1f0-bbcc-43ec-bfad-a9fac1bb0feb",
				},
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

	r := parquet.NewGenericReader[A](bytes.NewReader(b.Bytes()))
	values := make([]A, 1)
	_, err = r.Read(values)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expect[0], values[0]) {
		t.Errorf("want %q got %q", values[0], expect[0])
	}
}

func TestReadMinPageSize(t *testing.T) {
	// NOTE: min page size is 307 for MyRow schema
	t.Run("test read less than min page size", func(t *testing.T) { testReadMinPageSize(128, t) })
	t.Run("test read equal to min page size", func(t *testing.T) { testReadMinPageSize(307, t) })
	t.Run("test read more than min page size", func(t *testing.T) { testReadMinPageSize(384, t) })
	// NOTE: num rows is 20,000
	t.Run("test read equal to num rows", func(t *testing.T) { testReadMinPageSize(20_000, t) })
	t.Run("test read more than num rows", func(t *testing.T) { testReadMinPageSize(25_000, t) })
}

func testReadMinPageSize(readSize int, t *testing.T) {
	type MyRow struct {
		ID    [16]byte `parquet:"id,delta,uuid"`
		File  string   `parquet:"file,dict,zstd"`
		Index int64    `parquet:"index,delta,zstd"`
	}

	numRows := 20_000
	maxPageBytes := 5000

	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		t.Fatal("os.CreateTemp: ", err)
	}
	path := tmp.Name()
	defer os.Remove(path)
	t.Log("file:", path)

	// The page buffer size ensures we get multiple pages out of this example.
	w := parquet.NewGenericWriter[MyRow](tmp, parquet.PageBufferSize(maxPageBytes))
	// Need to write 1 row at a time here as writing many at once disregards PageBufferSize option.
	for i := 0; i < numRows; i++ {
		row := MyRow{
			ID:    [16]byte{15: byte(i)},
			File:  "hi" + fmt.Sprint(i),
			Index: int64(i),
		}
		_, err := w.Write([]MyRow{row})
		if err != nil {
			t.Fatal("w.Write: ", err)
		}
		// Flush writes rows as row group. 4 total (20k/5k) in this file.
		if (i+1)%maxPageBytes == 0 {
			err = w.Flush()
			if err != nil {
				t.Fatal("w.Flush: ", err)
			}
		}
	}
	err = w.Close()
	if err != nil {
		t.Fatal("w.Close: ", err)
	}
	err = tmp.Close()
	if err != nil {
		t.Fatal("tmp.Close: ", err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal("os.Open", err)
	}
	reader := parquet.NewGenericReader[MyRow](file)
	read := int64(0)
	nRows := reader.NumRows()
	rows := make([]MyRow, 0, nRows)
	buf := make([]MyRow, readSize) // NOTE: min page size is 307 for MyRow schema

	for read < nRows {
		num, err := reader.Read(buf)
		read += int64(num)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatal("Read:", err)
		}
		rows = append(rows, buf...)
	}

	if err := reader.Close(); err != nil {
		t.Fatal("Close", err)
	}

	if len(rows) < numRows {
		t.Fatalf("not enough values were read: want=%d got=%d", len(rows), numRows)
	}
	for i, row := range rows[:numRows] {
		id := [16]byte{15: byte(i)}
		file := "hi" + fmt.Sprint(i)
		index := int64(i)

		if row.ID != id || row.File != file || row.Index != index {
			t.Fatalf("rows mismatch at index: %d got: %+v", i, row)
		}
	}
}

func BenchmarkGenericReader(b *testing.B) {
	benchmarkGenericReader[benchmarkRowType](b)
	benchmarkGenericReader[booleanColumn](b)
	benchmarkGenericReader[int32Column](b)
	benchmarkGenericReader[int64Column](b)
	benchmarkGenericReader[floatColumn](b)
	benchmarkGenericReader[doubleColumn](b)
	benchmarkGenericReader[byteArrayColumn](b)
	benchmarkGenericReader[fixedLenByteArrayColumn](b)
	benchmarkGenericReader[stringColumn](b)
	benchmarkGenericReader[indexedStringColumn](b)
	benchmarkGenericReader[uuidColumn](b)
	benchmarkGenericReader[timeColumn](b)
	benchmarkGenericReader[timeInMillisColumn](b)
	benchmarkGenericReader[mapColumn](b)
	benchmarkGenericReader[decimalColumn](b)
	benchmarkGenericReader[contact](b)
	benchmarkGenericReader[paddedBooleanColumn](b)
	benchmarkGenericReader[optionalInt32Column](b)
}

func benchmarkGenericReader[Row generator[Row]](b *testing.B) {
	var model Row
	b.Run(reflect.TypeOf(model).Name(), func(b *testing.B) {
		prng := rand.New(rand.NewSource(0))
		rows := make([]Row, benchmarkNumRows)
		for i := range rows {
			rows[i] = rows[i].generate(prng)
		}

		rowbuf := make([]Row, benchmarkRowsPerStep)
		buffer := parquet.NewGenericBuffer[Row]()
		buffer.Write(rows)

		b.Run("go1.17", func(b *testing.B) {
			reader := parquet.NewRowGroupReader(buffer)
			benchmarkRowsPerSecond(b, func() int {
				for i := range rowbuf {
					if err := reader.Read(&rowbuf[i]); err != nil {
						if err != io.EOF {
							b.Fatal(err)
						} else {
							reader.Reset()
						}
					}
				}
				return len(rowbuf)
			})
		})

		b.Run("go1.18", func(b *testing.B) {
			reader := parquet.NewGenericRowGroupReader[Row](buffer)
			benchmarkRowsPerSecond(b, func() int {
				n, err := reader.Read(rowbuf)
				if err != nil {
					if err != io.EOF {
						b.Fatal(err)
					} else {
						reader.Reset()
					}
				}
				return n
			})
		})
	})
}
