package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet"
)

func TestMergeRowGroups(t *testing.T) {
	type schema struct {
		FirstName string
		LastName  string
		Age       int
	}

	makeRow := func(firstName, lastName string, age int) schema {
		return schema{FirstName: firstName, LastName: lastName, Age: age}
	}

	newRowGroup := func(options []parquet.RowGroupOption, rows ...schema) parquet.RowGroup {
		buf := parquet.NewBuffer(options...)
		for _, row := range rows {
			buf.Write(row)
		}
		return buf
	}

	tests := []struct {
		scenario string
		options  []parquet.RowGroupOption
		input    []parquet.RowGroup
		output   parquet.RowGroup
	}{
		{
			scenario: "merging an empty row group list produce no rows",
			output:   newRowGroup(nil),
		},

		{
			scenario: "merging a single row group returns one equal to the original",
			input: []parquet.RowGroup{
				newRowGroup(nil,
					makeRow("some", "one", 30),
					makeRow("some", "one else", 31),
					makeRow("and", "you", 32),
				),
			},
			output: newRowGroup(nil,
				makeRow("some", "one", 30),
				makeRow("some", "one else", 31),
				makeRow("and", "you", 32),
			),
		},

		{
			scenario: "merging two row groups produces the concatenation of their rows",
			input: []parquet.RowGroup{
				newRowGroup(nil, makeRow("some", "one", 30)),
				newRowGroup(nil, makeRow("some", "one else", 31)),
			},
			output: newRowGroup(nil,
				makeRow("some", "one", 30),
				makeRow("some", "one else", 31),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			merged, err := parquet.MergeRowGroups(test.input, test.options...)
			if err != nil {
				t.Fatal(err)
			}

			var expectedNumRows = test.output.NumRows()
			var mergedNumRows = merged.NumRows()
			if expectedNumRows != mergedNumRows {
				t.Fatalf("the number of rows mismatch: want=%d got=%d", expectedNumRows, mergedNumRows)
			}

			var expectedRows = test.output.Rows()
			var mergedRows = merged.Rows()
			var row1 parquet.Row
			var row2 parquet.Row
			var err1 error
			var err2 error
			var numRows int

			for {
				row1, err1 = expectedRows.ReadRow(row1[:0])
				row2, err2 = mergedRows.ReadRow(row2[:0])

				if err1 != err2 {
					t.Fatalf("errors mismatched while comparing rows: want=%v got=%v", err1, err2)
				}

				if err1 != nil {
					break
				}

				if !row1.Equal(row2) {
					t.Fatalf("row at index %d mismatch: want=%+v got=%+v", numRows, row1, row2)
				}

				numRows++
			}

			if numRows != expectedNumRows {
				t.Fatalf("expected to read %d rows but %d were found", expectedNumRows, numRows)
			}
		})
	}
}
