//go:build go1.18

package parquet_test

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/yonesko/parquet-go"
)

func TestGenericBuffer(t *testing.T) {
	testGenericBuffer[booleanColumn](t)
	testGenericBuffer[int32Column](t)
	testGenericBuffer[int64Column](t)
	testGenericBuffer[int96Column](t)
	testGenericBuffer[floatColumn](t)
	testGenericBuffer[doubleColumn](t)
	testGenericBuffer[byteArrayColumn](t)
	testGenericBuffer[fixedLenByteArrayColumn](t)
	testGenericBuffer[stringColumn](t)
	testGenericBuffer[indexedStringColumn](t)
	testGenericBuffer[uuidColumn](t)
	testGenericBuffer[mapColumn](t)
	testGenericBuffer[decimalColumn](t)
	testGenericBuffer[addressBook](t)
	testGenericBuffer[contact](t)
	testGenericBuffer[listColumn2](t)
	testGenericBuffer[listColumn1](t)
	testGenericBuffer[listColumn0](t)
	testGenericBuffer[nestedListColumn1](t)
	testGenericBuffer[nestedListColumn](t)
	testGenericBuffer[*contact](t)
	testGenericBuffer[paddedBooleanColumn](t)
	testGenericBuffer[optionalInt32Column](t)
	testGenericBuffer[repeatedInt32Column](t)
}

func testGenericBuffer[Row any](t *testing.T) {
	var model Row
	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		err := quickCheck(func(rows []Row) bool {
			if len(rows) == 0 {
				return true // TODO: fix support for parquet files with zero rows
			}
			if err := testGenericBufferRows(rows); err != nil {
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

func testGenericBufferRows[Row any](rows []Row) error {
	setNullPointers(rows)
	buffer := parquet.NewGenericBuffer[Row]()
	_, err := buffer.Write(rows)
	if err != nil {
		return err
	}
	reader := parquet.NewGenericRowGroupReader[Row](buffer)
	result := make([]Row, len(rows))
	n, err := reader.Read(result)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(rows) {
		return fmt.Errorf("not enough values were read: want=%d got=%d", len(rows), n)
	}
	if !reflect.DeepEqual(rows, result) {
		return fmt.Errorf("rows mismatch:\nwant: %#v\ngot:  %#v", rows, result)
	}
	return nil
}

func setNullPointers[Row any](rows []Row) {
	if len(rows) > 0 && reflect.TypeOf(rows[0]).Kind() == reflect.Pointer {
		for i := range rows {
			v := reflect.ValueOf(&rows[i]).Elem()
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
		}
	}
}

type generator[T any] interface {
	generate(*rand.Rand) T
}

func BenchmarkGenericBuffer(b *testing.B) {
	benchmarkGenericBuffer[benchmarkRowType](b)
	benchmarkGenericBuffer[booleanColumn](b)
	benchmarkGenericBuffer[int32Column](b)
	benchmarkGenericBuffer[int64Column](b)
	benchmarkGenericBuffer[floatColumn](b)
	benchmarkGenericBuffer[doubleColumn](b)
	benchmarkGenericBuffer[byteArrayColumn](b)
	benchmarkGenericBuffer[fixedLenByteArrayColumn](b)
	benchmarkGenericBuffer[stringColumn](b)
	benchmarkGenericBuffer[indexedStringColumn](b)
	benchmarkGenericBuffer[uuidColumn](b)
	benchmarkGenericBuffer[mapColumn](b)
	benchmarkGenericBuffer[decimalColumn](b)
	benchmarkGenericBuffer[contact](b)
	benchmarkGenericBuffer[paddedBooleanColumn](b)
	benchmarkGenericBuffer[optionalInt32Column](b)
	benchmarkGenericBuffer[repeatedInt32Column](b)
}

func benchmarkGenericBuffer[Row generator[Row]](b *testing.B) {
	var model Row
	b.Run(reflect.TypeOf(model).Name(), func(b *testing.B) {
		prng := rand.New(rand.NewSource(0))
		rows := make([]Row, benchmarkNumRows)
		for i := range rows {
			rows[i] = rows[i].generate(prng)
		}

		b.Run("go1.17", func(b *testing.B) {
			buffer := parquet.NewBuffer(parquet.SchemaOf(rows[0]))
			i := 0
			benchmarkRowsPerSecond(b, func() int {
				for j := 0; j < benchmarkRowsPerStep; j++ {
					if err := buffer.Write(&rows[i]); err != nil {
						b.Fatal(err)
					}
				}

				i += benchmarkRowsPerStep
				i %= benchmarkNumRows

				if i == 0 {
					buffer.Reset()
				}
				return benchmarkRowsPerStep
			})
		})

		b.Run("go1.18", func(b *testing.B) {
			buffer := parquet.NewGenericBuffer[Row]()
			i := 0
			benchmarkRowsPerSecond(b, func() int {
				n, err := buffer.Write(rows[i : i+benchmarkRowsPerStep])
				if err != nil {
					b.Fatal(err)
				}

				i += benchmarkRowsPerStep
				i %= benchmarkNumRows

				if i == 0 {
					buffer.Reset()
				}
				return n
			})
		})
	})
}
