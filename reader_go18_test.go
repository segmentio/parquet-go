//go:build go1.18

package parquet_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
		return fmt.Errorf("rows mismatch:\nwant: %+v\ngot:  %+v", rows, result)
	}
	return nil
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
