package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/google/uuid"
	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/deprecated"
)

type booleanColumn struct {
	Value bool
}

type int32Column struct {
	Value int32
}

type int64Column struct {
	Value int64
}

type int96Column struct {
	Value deprecated.Int96
}

type floatColumn struct {
	Value float32
}

type doubleColumn struct {
	Value float64
}

type byteArrayColumn struct {
	Value []byte
}

type fixedLenByteArrayColumn struct {
	Value [10]byte
}

type stringColumn struct {
	Value string
}

type indexedStringColumn struct {
	Value string `parquet:",dict"`
}

type uuidColumn struct {
	Value uuid.UUID
}

type decimalColumn struct {
	Value int64 `parquet:",decimal(0:3)"`
}

type addressBook struct {
	Owner             utf8string
	OwnerPhoneNumbers []utf8string
	Contacts          []contact
}

type contact struct {
	Name        utf8string
	PhoneNumber utf8string
}

type listColumn2 struct {
	Value utf8string `parquet:",optional"`
}

type listColumn1 struct {
	List2 []listColumn2 `parquet:",list"`
}

type listColumn0 struct {
	List1 []listColumn1 `parquet:",list"`
}

type nestedListColumn1 struct {
	Level3 []utf8string `parquet:"level3"`
}

type nestedListColumn struct {
	Level1 []nestedListColumn1 `parquet:"level1"`
	Level2 []utf8string        `parquet:"level2"`
}

type utf8string string

func (utf8string) Generate(rand *rand.Rand, size int) reflect.Value {
	const characters = "abcdefghijklmnopqrstuvwxyz1234567890"
	b := make([]byte, size)
	for i := range b {
		b[i] = characters[rand.Intn(len(characters))]
	}
	return reflect.ValueOf(utf8string(b))
}

func rowsOf(numRows int, model interface{}) rows {
	typ := reflect.TypeOf(model)
	prng := rand.New(rand.NewSource(0))
	rows := make(rows, numRows)
	for i := range rows {
		v, ok := quick.Value(typ, prng)
		if !ok {
			panic("cannot generate random value for test")
		}
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

					if err := writeParquetFile(buf, rows); err != nil {
						t.Fatal(err)
					}

					file.Reset(buf.Bytes())
					r := parquet.NewReader(file)

					for i, v := range rows {
						if err := r.ReadRow(rowPtr.Interface()); err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(rowValue.Interface(), v) {
							t.Errorf("row mismatch at index %d\nwant = %+v\ngot  = %+v", i, v, rowValue.Interface())
						}
						rowValue.Set(rowZero)
					}

					if err := r.ReadRow(rowPtr.Interface()); err != io.EOF {
						t.Errorf("expected EOF after reading all values but got: %v", err)
					}
				})
			}
		})
	}
}

func BenchmarkReader(b *testing.B) {
	buf := new(bytes.Buffer)
	file := bytes.NewReader(nil)

	for _, test := range readerTests {
		b.Run(test.scenario, func(b *testing.B) {
			const N = 2042
			defer buf.Reset()
			rows := rowsOf(N, test.model)

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

			b.ResetTimer()
			r := parquet.NewReader(f)
			p := rowPtr.Interface()

			for i := 0; i < b.N; i++ {
				if err := r.ReadRow(p); err != nil {
					if err == io.EOF {
						r.Reset()
					} else {
						b.Fatalf("%d/%d: %v", i, b.N, err)
					}
				}
				rowValue.Set(rowZero)
			}

			b.SetBytes(int64(math.Ceil(float64(file.Size()) / N)))
		})
	}
}
