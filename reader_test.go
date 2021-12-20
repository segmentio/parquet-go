package parquet_test

import (
	"bytes"
	"io"
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
}

func TestReader(t *testing.T) {
	buf := new(bytes.Buffer)
	file := bytes.NewReader(nil)

	for _, test := range readerTests {
		t.Run(test.scenario, func(t *testing.T) {
			defer buf.Reset()
			const N = 10
			rows := rowsOf(N, test.model)

			if err := writeParquetFile(buf, rows); err != nil {
				t.Fatal(err)
			}

			file.Reset(buf.Bytes())
			r := parquet.NewReader(file, file.Size())
			rowType := reflect.TypeOf(rows[0])
			rowPtr := reflect.New(rowType)
			rowZero := reflect.Zero(rowType)
			rowValue := rowPtr.Elem()

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
}
