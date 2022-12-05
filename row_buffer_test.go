//go:build go1.18

package parquet_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestRowBuffer(t *testing.T) {
	testRowBuffer[booleanColumn](t)
	testRowBuffer[int32Column](t)
	testRowBuffer[int64Column](t)
	testRowBuffer[int96Column](t)
	testRowBuffer[floatColumn](t)
	testRowBuffer[doubleColumn](t)
	testRowBuffer[byteArrayColumn](t)
	testRowBuffer[fixedLenByteArrayColumn](t)
	testRowBuffer[stringColumn](t)
	testRowBuffer[indexedStringColumn](t)
	testRowBuffer[uuidColumn](t)
	testRowBuffer[timeColumn](t)
	testRowBuffer[timeInMillisColumn](t)
	testRowBuffer[mapColumn](t)
	testRowBuffer[decimalColumn](t)
	testRowBuffer[addressBook](t)
	testRowBuffer[contact](t)
	testRowBuffer[listColumn2](t)
	testRowBuffer[listColumn1](t)
	testRowBuffer[listColumn0](t)
	testRowBuffer[nestedListColumn1](t)
	testRowBuffer[nestedListColumn](t)
	testRowBuffer[*contact](t)
	testRowBuffer[paddedBooleanColumn](t)
	testRowBuffer[optionalInt32Column](t)
	testRowBuffer[repeatedInt32Column](t)
}

func testRowBuffer[Row any](t *testing.T) {
	var model Row
	t.Run(reflect.TypeOf(model).Name(), func(t *testing.T) {
		err := quickCheck(func(rows []Row) bool {
			if len(rows) == 0 {
				return true // TODO: fix support for parquet files with zero rows
			}
			if err := testRowBufferRows(rows); err != nil {
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

func testRowBufferRows[Row any](rows []Row) error {
	setNullPointers(rows)
	buffer := parquet.NewRowBuffer[Row]()
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

func BenchmarkSortRowBuffer(b *testing.B) {
	type Row struct {
		I0 int64
		I1 int64
		I2 int64
		I3 int64
		I4 int64
		I5 int64
		I6 int64
		I7 int64
		I8 int64
		I9 int64
		ID [16]byte
	}

	buf := parquet.NewRowBuffer[Row](
		parquet.SortingColumns(
			parquet.Ascending("ID"),
		),
	)

	rows := make([]Row, 10e3)
	prng := rand.New(rand.NewSource(0))

	for i := range rows {
		binary.LittleEndian.PutUint64(rows[i].ID[:8], uint64(i))
		binary.LittleEndian.PutUint64(rows[i].ID[8:], ^uint64(i))
	}

	buf.Write(rows)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			buf.Swap(prng.Intn(len(rows)), prng.Intn(len(rows)))
		}

		sort.Sort(buf)
	}
}
