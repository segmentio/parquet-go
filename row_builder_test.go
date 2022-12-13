package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestRowBuilder(t *testing.T) {
	type add struct {
		columnIndex int
		columnValue parquet.Value
	}

	tests := []struct {
		scenario string
		adds     []add
		want     parquet.Row
		schema   parquet.Node
	}{
		{
			scenario: "add missing required column value",
			adds:     nil,
			want: parquet.Row{
				parquet.Int64Value(0).Level(0, 0, 0),
			},
			schema: parquet.Group{
				"id": parquet.Int(64),
			},
		},

		{
			scenario: "set required column value",
			adds: []add{
				{columnIndex: 0, columnValue: parquet.Int64Value(1)},
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
			},
			schema: parquet.Group{
				"id": parquet.Int(64),
			},
		},

		{
			scenario: "set repeated column values",
			adds: []add{
				{columnIndex: 0, columnValue: parquet.Int64Value(1)},
				{columnIndex: 1, columnValue: parquet.ByteArrayValue([]byte(`1`))},
				{columnIndex: 1, columnValue: parquet.ByteArrayValue([]byte(`2`))},
				{columnIndex: 1, columnValue: parquet.ByteArrayValue([]byte(`3`))},
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
				parquet.ByteArrayValue([]byte(`1`)).Level(0, 1, 1),
				parquet.ByteArrayValue([]byte(`2`)).Level(1, 1, 1),
				parquet.ByteArrayValue([]byte(`3`)).Level(1, 1, 1),
			},
			schema: parquet.Group{
				"id":    parquet.Int(64),
				"names": parquet.Repeated(parquet.String()),
			},
		},

		{
			scenario: "add missing repeated column value",
			adds: []add{
				{columnIndex: 0, columnValue: parquet.Int64Value(1)},
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
				parquet.ByteArrayValue(nil).Level(0, 0, 1),
			},
			schema: parquet.Group{
				"id":    parquet.Int(64),
				"names": parquet.Repeated(parquet.String()),
			},
		},

		{
			scenario: "add missing optional column value",
			adds: []add{
				{columnIndex: 0, columnValue: parquet.Int64Value(1)},
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
				parquet.ByteArrayValue(nil).Level(0, 0, 1),
			},
			schema: parquet.Group{
				"id":   parquet.Int(64),
				"name": parquet.Optional(parquet.String()),
			},
		},

		{
			scenario: "add missing nested column values",
			adds: []add{
				{columnIndex: 0, columnValue: parquet.Int64Value(1)},
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),
				parquet.Int32Value(0).Level(0, 0, 1),
				parquet.ByteArrayValue(nil).Level(0, 0, 2),
				parquet.ByteArrayValue(nil).Level(0, 0, 3),
			},
			schema: parquet.Group{
				"id": parquet.Int(64),
				"profile": parquet.Group{
					"first_name": parquet.String(),
					"last_name":  parquet.String(),
					"birth_date": parquet.Optional(parquet.Date()),
				},
			},
		},
		{
			scenario: "add missing repeated column group",
			adds: []add{
				{columnIndex: 0, columnValue: parquet.Int64Value(1)},
				{columnIndex: 2, columnValue: parquet.ByteArrayValue([]byte(`me`))},
				{columnIndex: 1, columnValue: parquet.Int32Value(0)},
				{columnIndex: 1, columnValue: parquet.Int32Value(123456)},
				{columnIndex: 2, columnValue: parquet.ByteArrayValue([]byte(`you`))},
			},
			want: parquet.Row{
				parquet.Int64Value(1).Level(0, 0, 0),

				parquet.Int32Value(0).Level(0, 2, 1),
				parquet.Int32Value(123456).Level(1, 2, 1),

				parquet.ByteArrayValue([]byte(`me`)).Level(0, 1, 2),
				parquet.ByteArrayValue([]byte(`you`)).Level(1, 1, 2),

				parquet.ByteArrayValue(nil).Level(0, 1, 3),
				parquet.ByteArrayValue(nil).Level(1, 1, 3),
			},
			schema: parquet.Group{
				"id": parquet.Int(64),
				"profiles": parquet.Repeated(parquet.Group{
					"first_name": parquet.String(),
					"last_name":  parquet.String(),
					"birth_date": parquet.Optional(parquet.Date()),
				}),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			b := parquet.NewRowBuilder(test.schema)

			for i := 0; i < 2; i++ {
				for _, add := range test.adds {
					b.Add(add.columnIndex, add.columnValue)
				}

				if got := b.Row(); !got.Equal(test.want) {
					t.Fatalf("test %d: rows are not equal\nwant = %+v\ngot  = %+v", i+1, test.want, got)
				}

				b.Reset()
			}
		})
	}
}
