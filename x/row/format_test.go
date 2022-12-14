package row

import (
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/deprecated"
)

func TestMarshalUnmarshal(t *testing.T) {
	tests := []struct {
		scenario string
		row      parquet.Row
	}{
		{
			scenario: "invalid empty row",
		},

		{
			scenario: "row with one boolean value",
			row: parquet.Row{
				parquet.BooleanValue(true).Level(0, 0, 0),
			},
		},

		{
			scenario: "row with one int32 value",
			row: parquet.Row{
				parquet.Int32Value(42).Level(0, 0, 0),
			},
		},

		{
			scenario: "row with one int64 value",
			row: parquet.Row{
				parquet.Int64Value(123).Level(0, 0, 0),
			},
		},

		{
			scenario: "row with one int96 value",
			row: parquet.Row{
				parquet.Int96Value(deprecated.Int96{0: 1}).Level(0, 0, 0),
			},
		},

		{
			scenario: "row with one float value",
			row: parquet.Row{
				parquet.FloatValue(0.5).Level(0, 0, 0),
			},
		},

		{
			scenario: "row with one double value",
			row: parquet.Row{
				parquet.DoubleValue(-0.5).Level(0, 0, 0),
			},
		},

		{
			scenario: "row with one byte array value",
			row: parquet.Row{
				parquet.ByteArrayValue([]byte(`Hello World!`)).Level(0, 0, 0),
			},
		},

		{
			scenario: "row with one fixed length byte array value",
			row: parquet.Row{
				parquet.FixedLenByteArrayValue([]byte(`9A0AC5AFECB0485592E06F962CFF4D12`)).Level(0, 0, 0),
			},
		},

		{
			scenario: "row with one null value",
			row: parquet.Row{
				parquet.NullValue().Level(1, 2, 0),
			},
		},

		/*
			{
				scenario: "row with multiple columns",
				row: parquet.Row{
					//parquet.Int64Value(0).Level(0, 0, 0),
					//parquet.ByteArrayValue([]byte(`hello`)).Level(0, 1, 1),
					//parquet.ByteArrayValue([]byte(`world`)).Level(0, 1, 2),
					parquet.Int32Value(0).Level(0, 1, 3),
					parquet.Int32Value(1).Level(1, 1, 3),
					parquet.Int32Value(2).Level(1, 1, 3),
					parquet.Int32Value(3).Level(1, 1, 3),
					parquet.NullValue().Level(1, 0, 3),
				},
			},
		*/
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			buf, err := marshalAppend(nil, test.row)
			if err != nil {
				t.Fatal("marshal:", err)
			}
			got, err := unmarshalAppend(nil, buf)
			if err != nil {
				t.Fatal("unmarshal:", err)
			}
			if want := test.row; !got.Equal(want) {
				t.Errorf("rows mismatch:\nwant = %+v\ngot  = %+v", want, got)
			}
		})
	}
}

func BenchmarkMarshal(b *testing.B) {
	buf := make([]byte, 1024)
	row := parquet.Row{
		parquet.Int64Value(0).Level(0, 0, 0),
		parquet.ByteArrayValue([]byte(`hello`)).Level(0, 1, 1),
		parquet.ByteArrayValue([]byte(`world`)).Level(0, 1, 2),
		parquet.Int32Value(0).Level(0, 1, 3),
		parquet.Int32Value(1).Level(1, 1, 3),
		parquet.Int32Value(2).Level(1, 1, 3),
		parquet.Int32Value(3).Level(1, 1, 3),
		parquet.NullValue().Level(1, 0, 3),
	}

	for i := 0; i < b.N; i++ {
		buf, _ = marshalAppend(buf[:0], row)
	}
}
