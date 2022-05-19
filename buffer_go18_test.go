package parquet_test

import (
	"errors"
	"io"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/segmentio/parquet-go"
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
	testGenericBuffer[decimalColumn](t)
	testGenericBuffer[addressBook](t)
	testGenericBuffer[contact](t)
	testGenericBuffer[listColumn2](t)
	testGenericBuffer[listColumn1](t)
	testGenericBuffer[listColumn0](t)
	testGenericBuffer[nestedListColumn1](t)
	testGenericBuffer[nestedListColumn](t)
}

func testGenericBuffer[Row any](t *testing.T) {
	var prng = rand.New(rand.NewSource(1))
	var model Row
	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		f := func(rows []Row) bool {
			if len(rows) == 0 {
				return true // TODO: fix support for parquet files with zero rows
			}
			buffer := parquet.NewGenericBuffer[Row]()
			_, err := buffer.Write(rows)
			if err != nil {
				t.Error(err)
				return false
			}
			reader := parquet.NewGenericRowGroupReader[Row](buffer)
			result := make([]Row, len(rows))
			n, err := reader.Read(result)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Error(err)
				return false
			}
			if n < len(rows) {
				t.Errorf("not enough values were read: want=%d got=%d", len(rows), n)
				return false
			}
			if !reflect.DeepEqual(rows, result) {
				t.Errorf("rows mismatch:\nwant: %+v\ngot:  %+v", rows, result)
				return false
			}
			return true
		}
		if err := quick.Check(f, &quick.Config{Rand: prng}); err != nil {
			t.Error(err)
		}
	})
}
