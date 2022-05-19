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

func BenchmarkGenericBuffer(b *testing.B) {
	data := make([]byte, 16*benchmarkReaderNumRows)
	prng := rand.New(rand.NewSource(0))
	prng.Read(data)

	values := make([]uuidColumn, benchmarkReaderNumRows)
	for i := range values {
		j := (i + 0) * 16
		k := (i + 1) * 16
		copy(values[i].Value[:], data[j:k])
	}

	b.Run("go1.17", func(b *testing.B) {
		buffer := parquet.NewBuffer(parquet.SchemaOf(uuidColumn{}))
		i := 0
		benchmarkRowsPerSecond(b, func() int {
			for j := 0; j < benchmarkBufferRowsPerStep; j++ {
				if err := buffer.Write(&values[i]); err != nil {
					b.Fatal(err)
				}
			}

			i += benchmarkBufferRowsPerStep
			i %= benchmarkBufferNumRows

			if i == 0 {
				buffer.Reset()
			}
			return benchmarkBufferRowsPerStep
		})
	})

	b.Run("go1.18", func(b *testing.B) {
		buffer := parquet.NewGenericBuffer[uuidColumn]()
		i := 0
		benchmarkRowsPerSecond(b, func() int {
			n, err := buffer.Write(values[i : i+benchmarkBufferRowsPerStep])
			if err != nil {
				b.Fatal(err)
			}

			i += benchmarkBufferRowsPerStep
			i %= benchmarkBufferNumRows

			if i == 0 {
				buffer.Reset()
			}
			return n
		})
	})
}
