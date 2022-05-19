package parquet_test

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"testing/quick"

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
	testGenericReader[decimalColumn](t)
	testGenericReader[addressBook](t)
	testGenericReader[contact](t)
	testGenericReader[listColumn2](t)
	testGenericReader[listColumn1](t)
	testGenericReader[listColumn0](t)
	testGenericReader[nestedListColumn1](t)
	testGenericReader[nestedListColumn](t)
}

func testGenericReader[Row any](t *testing.T) {
	var model Row
	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		f := func(rows []Row) bool {
			if len(rows) == 0 {
				return true // TODO: fix support for parquet files with zero rows
			}
			buffer := new(bytes.Buffer)
			writer := parquet.NewGenericWriter[Row](buffer)
			_, err := writer.Write(rows)
			if err != nil {
				t.Error(err)
				return false
			}
			if err := writer.Close(); err != nil {
				t.Error(err)
				return false
			}
			reader := parquet.NewGenericReader[Row](bytes.NewReader(buffer.Bytes()))
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
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	})
}
