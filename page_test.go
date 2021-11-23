package parquet_test

import (
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/segmentio/parquet"
)

var pageReadWriteTests = []struct {
	scenario string
	typ      parquet.Type
	values   [][]parquet.Value
}{
	{
		scenario: "boolean",
		typ:      parquet.BooleanType,
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf(false)},
			{parquet.ValueOf(true)},
			{
				parquet.ValueOf(false),
				parquet.ValueOf(true),
				parquet.ValueOf(false),
				parquet.ValueOf(false),
				parquet.ValueOf(true),
				parquet.ValueOf(true),
				parquet.ValueOf(false),
				parquet.ValueOf(false),
				parquet.ValueOf(false),
				parquet.ValueOf(true),
				parquet.ValueOf(false),
				parquet.ValueOf(true),
				parquet.ValueOf(false),
			},
		},
	},

	{
		scenario: "int32",
		typ:      parquet.Int32Type,
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf(int32(0))},
			{parquet.ValueOf(int32(1))},
			{
				parquet.ValueOf(int32(1)),
				parquet.ValueOf(int32(2)),
				parquet.ValueOf(int32(3)),
				parquet.ValueOf(int32(4)),
				parquet.ValueOf(int32(5)),
				parquet.ValueOf(int32(6)),
				parquet.ValueOf(int32(math.MaxInt8)),
				parquet.ValueOf(int32(math.MaxInt16)),
				parquet.ValueOf(int32(math.MaxInt32)),
				parquet.ValueOf(int32(7)),
				parquet.ValueOf(int32(9)),
				parquet.ValueOf(int32(9)),
				parquet.ValueOf(int32(0)),
			},
		},
	},

	{
		scenario: "int64",
		typ:      parquet.Int64Type,
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf(int64(0))},
			{parquet.ValueOf(int64(1))},
			{
				parquet.ValueOf(int64(1)),
				parquet.ValueOf(int64(2)),
				parquet.ValueOf(int64(3)),
				parquet.ValueOf(int64(4)),
				parquet.ValueOf(int64(5)),
				parquet.ValueOf(int64(6)),
				parquet.ValueOf(int64(math.MaxInt8)),
				parquet.ValueOf(int64(math.MaxInt16)),
				parquet.ValueOf(int64(math.MaxInt64)),
				parquet.ValueOf(int64(7)),
				parquet.ValueOf(int64(9)),
				parquet.ValueOf(int64(9)),
				parquet.ValueOf(int64(0)),
			},
		},
	},

	{
		scenario: "float",
		typ:      parquet.FloatType,
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf(float32(0))},
			{parquet.ValueOf(float32(1))},
			{
				parquet.ValueOf(float32(1)),
				parquet.ValueOf(float32(2)),
				parquet.ValueOf(float32(3)),
				parquet.ValueOf(float32(4)),
				parquet.ValueOf(float32(5)),
				parquet.ValueOf(float32(6)),
				parquet.ValueOf(float32(0.5)),
				parquet.ValueOf(float32(math.SmallestNonzeroFloat32)),
				parquet.ValueOf(float32(math.MaxFloat32)),
				parquet.ValueOf(float32(7)),
				parquet.ValueOf(float32(9)),
				parquet.ValueOf(float32(9)),
				parquet.ValueOf(float32(0)),
			},
		},
	},

	{
		scenario: "double",
		typ:      parquet.DoubleType,
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf(float64(0))},
			{parquet.ValueOf(float64(1))},
			{
				parquet.ValueOf(float64(1)),
				parquet.ValueOf(float64(2)),
				parquet.ValueOf(float64(3)),
				parquet.ValueOf(float64(4)),
				parquet.ValueOf(float64(5)),
				parquet.ValueOf(float64(6)),
				parquet.ValueOf(float64(0.5)),
				parquet.ValueOf(float64(math.SmallestNonzeroFloat64)),
				parquet.ValueOf(float64(math.MaxFloat64)),
				parquet.ValueOf(float64(7)),
				parquet.ValueOf(float64(9)),
				parquet.ValueOf(float64(9)),
				parquet.ValueOf(float64(0)),
			},
		},
	},

	{
		scenario: "string",
		typ:      parquet.ByteArrayType,
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf("")},
			{parquet.ValueOf("Hello World!")},
			{
				parquet.ValueOf("ABCDEFG"),
				parquet.ValueOf("HIJKLMN"),
				parquet.ValueOf("OPQRSTU"),
				parquet.ValueOf("VWXZY01"),
				parquet.ValueOf("2345678"),
				parquet.ValueOf("90!@#$%"),
				parquet.ValueOf("^&*()_+"),
				parquet.ValueOf("Hello World!"),
				parquet.ValueOf("Answer=42"),
				parquet.ValueOf("ABCEDFG"),
				parquet.ValueOf("HIJKLMN"),
				parquet.ValueOf("OPQRSTU"),
				parquet.ValueOf("VWXYZ"),
			},
		},
	},

	{
		scenario: "uuid",
		typ:      parquet.UUID().Type(),
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf([16]byte{})},
			{parquet.ValueOf([16]byte{0: 1})},
			{
				parquet.ValueOf([16]byte{0: 0}),
				parquet.ValueOf([16]byte{0: 2}),
				parquet.ValueOf([16]byte{0: 1}),
				parquet.ValueOf([16]byte{0: 4}),
				parquet.ValueOf([16]byte{0: 3}),
				parquet.ValueOf([16]byte{0: 6}),
				parquet.ValueOf([16]byte{0: 5}),
				parquet.ValueOf([16]byte{0: 8}),
				parquet.ValueOf([16]byte{0: 7}),
				parquet.ValueOf([16]byte{0: 10}),
				parquet.ValueOf([16]byte{0: 11}),
				parquet.ValueOf([16]byte{0: 12}),
				parquet.ValueOf([16]byte{15: 0xFF}),
			},
		},
	},

	{
		scenario: "uint32",
		typ:      parquet.Uint(32).Type(),
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf(uint32(0))},
			{parquet.ValueOf(uint32(1))},
			{
				parquet.ValueOf(uint32(1)),
				parquet.ValueOf(uint32(2)),
				parquet.ValueOf(uint32(3)),
				parquet.ValueOf(uint32(4)),
				parquet.ValueOf(uint32(5)),
				parquet.ValueOf(uint32(6)),
				parquet.ValueOf(uint32(math.MaxInt8)),
				parquet.ValueOf(uint32(math.MaxInt16)),
				parquet.ValueOf(uint32(math.MaxUint32)),
				parquet.ValueOf(uint32(7)),
				parquet.ValueOf(uint32(9)),
				parquet.ValueOf(uint32(9)),
				parquet.ValueOf(uint32(0)),
			},
		},
	},

	{
		scenario: "uint64",
		typ:      parquet.Uint(64).Type(),
		values: [][]parquet.Value{
			{},
			{parquet.ValueOf(uint64(0))},
			{parquet.ValueOf(uint64(1))},
			{
				parquet.ValueOf(uint64(1)),
				parquet.ValueOf(uint64(2)),
				parquet.ValueOf(uint64(3)),
				parquet.ValueOf(uint64(4)),
				parquet.ValueOf(uint64(5)),
				parquet.ValueOf(uint64(6)),
				parquet.ValueOf(uint64(math.MaxInt8)),
				parquet.ValueOf(uint64(math.MaxInt16)),
				parquet.ValueOf(uint64(math.MaxUint64)),
				parquet.ValueOf(uint64(7)),
				parquet.ValueOf(uint64(9)),
				parquet.ValueOf(uint64(9)),
				parquet.ValueOf(uint64(0)),
			},
		},
	},
}

func TestPageReadWrite(t *testing.T) {
	buf := new(bytes.Buffer)
	dec := parquet.Plain.NewDecoder(buf)
	enc := parquet.Plain.NewEncoder(buf)

	for _, test := range pageReadWriteTests {
		t.Run(test.scenario, func(t *testing.T) {
			pageReader := test.typ.NewPageReader(dec, 32)
			pageWriter := test.typ.NewPageWriter(enc, 32)

			for _, values := range test.values {
				t.Run("", func(t *testing.T) {
					defer pageWriter.Reset(enc)
					defer pageReader.Reset(dec)
					defer enc.Reset(buf)
					defer dec.Reset(buf)

					minValue := parquet.Value{}
					maxValue := parquet.Value{}

					for _, value := range values {
						if err := pageWriter.WriteValue(value); err != nil {
							t.Fatal("writing value to page writer:", err)
						}
						if minValue.IsNull() || test.typ.Less(value, minValue) {
							minValue = value
						}
						if maxValue.IsNull() || test.typ.Less(maxValue, value) {
							maxValue = value
						}
					}

					min, max := pageWriter.Bounds()
					if !parquet.Equal(min, minValue) {
						t.Errorf("min value mismatch: want=%v got=%v", minValue, min)
					}
					if !parquet.Equal(max, maxValue) {
						t.Errorf("max value mismatch: want=%v got=%v", maxValue, max)
					}

					if err := pageWriter.Flush(); err != nil {
						t.Fatal("flushing page writer:", err)
					}

					if err := enc.Close(); err != nil {
						t.Fatal("closing encoder:", err)
					}

					n := pageWriter.NumValues()
					i := 0
					for {
						v, err := pageReader.ReadValue()
						if err != nil {
							if err == io.EOF {
								break
							}
							t.Fatal("reading value from page reader:", err)
						}

						if i < len(values) {
							if !parquet.Equal(v, values[i]) {
								t.Errorf("value at index %d mismatches: want=%v got=%v", i, values[i], v)
							}
						}

						i++
					}

					if i != n {
						t.Errorf("wrong number of values read from page reader: want=%d got=%d", n, i)
					}
				})
			}
		})
	}
}
