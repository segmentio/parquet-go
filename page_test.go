package parquet_test

import (
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/encoding"
)

var pageReadWriteTests = []struct {
	scenario string
	typ      parquet.Type
	values   [][]interface{}
}{
	{
		scenario: "boolean",
		typ:      parquet.BooleanType,
		values: [][]interface{}{
			{},
			{false},
			{true},
			{
				false, true, false, false, true, true,
				false, false, false, true, false, true,
			},
		},
	},

	{
		scenario: "int32",
		typ:      parquet.Int32Type,
		values: [][]interface{}{
			{},
			{int32(0)},
			{int32(1)},
			{
				int32(1), int32(2), int32(3), int32(4), int32(5), int32(6),
				int32(math.MaxInt8), int32(math.MaxInt16), int32(math.MaxInt32),
				int32(7), int32(9), int32(9), int32(0),
			},
		},
	},

	{
		scenario: "int64",
		typ:      parquet.Int64Type,
		values: [][]interface{}{
			{},
			{int64(0)},
			{int64(1)},
			{
				int64(1), int64(2), int64(3), int64(4), int64(5), int64(6),
				int64(math.MaxInt8), int64(math.MaxInt16), int64(math.MaxInt64), int64(7),
				int64(9), int64(9), int64(0),
			},
		},
	},

	{
		scenario: "float",
		typ:      parquet.FloatType,
		values: [][]interface{}{
			{},
			{float32(0)},
			{float32(1)},
			{
				float32(1), float32(2), float32(3), float32(4), float32(5), float32(6),
				float32(0.5), float32(math.SmallestNonzeroFloat32), float32(math.MaxFloat32), float32(7),
				float32(9), float32(9), float32(0),
			},
		},
	},

	{
		scenario: "double",
		typ:      parquet.DoubleType,
		values: [][]interface{}{
			{},
			{float64(0)},
			{float64(1)},
			{
				float64(1), float64(2), float64(3), float64(4), float64(5), float64(6),
				float64(0.5), float64(math.SmallestNonzeroFloat64), float64(math.MaxFloat64), float64(7),
				float64(9), float64(9), float64(0),
			},
		},
	},

	{
		scenario: "string",
		typ:      parquet.ByteArrayType,
		values: [][]interface{}{
			{},
			{""},
			{"Hello World!"},
			{
				"ABCDEFG", "HIJKLMN", "OPQRSTU", "VWXZY01", "2345678",
				"90!@#$%", "^&*()_+", "Hello World!", "Answer=42", "ABCEDFG",
				"HIJKLMN", "OPQRSTU", "VWXYZ",
			},
		},
	},

	{
		scenario: "uuid",
		typ:      parquet.UUID().Type(),
		values: [][]interface{}{
			{},
			{[16]byte{}},
			{[16]byte{0: 1}},
			{
				[16]byte{0: 0}, [16]byte{0: 2}, [16]byte{0: 1}, [16]byte{0: 4}, [16]byte{0: 3},
				[16]byte{0: 6}, [16]byte{0: 5}, [16]byte{0: 8}, [16]byte{0: 7}, [16]byte{0: 10},
				[16]byte{0: 11}, [16]byte{0: 12}, [16]byte{15: 0xFF},
			},
		},
	},

	{
		scenario: "uint32",
		typ:      parquet.Uint(32).Type(),
		values: [][]interface{}{
			{},
			{uint32(0)},
			{uint32(1)},
			{
				uint32(1), uint32(2), uint32(3), uint32(4), uint32(5), uint32(6),
				uint32(math.MaxInt8), uint32(math.MaxInt16), uint32(math.MaxUint32), uint32(7),
				uint32(9), uint32(9), uint32(0),
			},
		},
	},

	{
		scenario: "uint64",
		typ:      parquet.Uint(64).Type(),
		values: [][]interface{}{
			{},
			{uint64(0)},
			{uint64(1)},
			{
				uint64(1), uint64(2), uint64(3), uint64(4), uint64(5), uint64(6),
				uint64(math.MaxInt8), uint64(math.MaxInt16), uint64(math.MaxUint64),
				uint64(7), uint64(9), uint64(9), uint64(0),
			},
		},
	},
}

func TestPageReadWrite(t *testing.T) {
	for _, test := range pageReadWriteTests {
		t.Run(test.scenario, func(t *testing.T) {
			for _, config := range [...]struct {
				scenario string
				typ      parquet.Type
			}{
				{scenario: "plain", typ: test.typ},
				{scenario: "indexed", typ: test.typ.NewDictionary(0).Type()},
			} {
				t.Run(config.scenario, func(t *testing.T) {
					buf := new(bytes.Buffer)
					dec := parquet.Plain.NewDecoder(buf)
					enc := parquet.Plain.NewEncoder(buf)
					pr := config.typ.NewValueDecoder(32)
					pw := config.typ.NewRowGroupColumn(1024)

					for _, values := range test.values {
						t.Run("", func(t *testing.T) {
							buf.Reset()
							dec.Reset(buf)
							enc.Reset(buf)
							pr.Reset(dec)
							pw.Reset()
							testPageReadWrite(t, config.typ, pr, pw, enc, values)
						})
					}
				})
			}
		})
	}
}

func testPageReadWrite(t *testing.T, typ parquet.Type, r parquet.ValueReader, w parquet.RowGroupColumn, e encoding.Encoder, values []interface{}) {
	minValue := parquet.Value{}
	maxValue := parquet.Value{}
	batch := make([]parquet.Value, len(values))
	for i := range values {
		batch[i] = parquet.ValueOf(values[i])
	}

	if n, err := w.WriteValues(batch); err != nil {
		t.Fatal("writing value to page writer:", err)
	} else if n != len(batch) {
		t.Fatalf("wrong number of values written: want=%d got=%d", len(batch), n)
	}

	for _, value := range batch {
		if minValue.IsNull() || typ.Less(value, minValue) {
			minValue = value
		}
		if maxValue.IsNull() || typ.Less(maxValue, value) {
			maxValue = value
		}
	}

	p := w.Page()
	numValues := p.NumValues()
	if numValues != len(values) {
		t.Errorf("number of values mistmatch: want=%d got=%d", len(values), numValues)
	}

	numNulls := p.NumNulls()
	if numNulls != 0 {
		t.Errorf("number of nulls mismatch: want=0 got=%d", numNulls)
	}

	min, max := p.Bounds()
	if !parquet.Equal(min, minValue) {
		t.Errorf("min value mismatch: want=%v got=%v", minValue, min)
	}
	if !parquet.Equal(max, maxValue) {
		t.Errorf("max value mismatch: want=%v got=%v", maxValue, max)
	}

	if err := p.WriteTo(e); err != nil {
		t.Fatal("flushing page writer:", err)
	}

	for _, test := range [...]struct {
		scenario string
		values   parquet.ValueReader
	}{
		{scenario: "page", values: p.Values()},
		{scenario: "test", values: r},
	} {
		v := [1]parquet.Value{}
		i := 0

		for {
			n, err := test.values.ReadValues(v[:])
			if n > 0 {
				if n != 1 {
					t.Fatalf("reading value from %q reader returned the wrong count: want=1 got=%d", test.scenario, n)
				}
				if i < len(values) {
					if !parquet.Equal(v[0], parquet.ValueOf(values[i])) {
						t.Errorf("%q value at index %d mismatches: want=%v got=%v", test.scenario, i, values[i], v[0])
					}
				}
				i++
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("reading value from %q reader: %v", test.scenario, err)
			}
		}

		if i != numValues {
			t.Errorf("wrong number of values read from %q reader: want=%d got=%d", test.scenario, numValues, i)
		}
	}
}
