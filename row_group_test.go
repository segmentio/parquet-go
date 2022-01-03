package parquet_test

import (
	"sort"
	"testing"

	"github.com/segmentio/parquet"
)

func TestMergeRowGroups(t *testing.T) {
	type dataType struct {
		FirstName string
		LastName  string
		Age       int
	}

	type subDataType struct {
		LastName string
	}

	newRowGroup := func(options []parquet.RowGroupOption, rows ...interface{}) parquet.RowGroup {
		buf := parquet.NewBuffer(options...)
		for _, row := range rows {
			buf.Write(row)
		}
		sort.Stable(buf)
		return buf
	}

	tests := []struct {
		scenario string
		options  []parquet.RowGroupOption
		input    []parquet.RowGroup
		output   parquet.RowGroup
	}{
		{
			scenario: "no row groups",
			options: []parquet.RowGroupOption{
				parquet.SchemaOf(dataType{}),
			},
			output: newRowGroup(
				[]parquet.RowGroupOption{
					parquet.SchemaOf(dataType{}),
				},
			),
		},

		{
			scenario: "a single row group",
			input: []parquet.RowGroup{
				newRowGroup(nil,
					dataType{FirstName: "some", LastName: "one", Age: 30},
					dataType{FirstName: "some", LastName: "one else", Age: 31},
					dataType{FirstName: "and", LastName: "you", Age: 32},
				),
			},
			output: newRowGroup(nil,
				dataType{FirstName: "some", LastName: "one", Age: 30},
				dataType{FirstName: "some", LastName: "one else", Age: 31},
				dataType{FirstName: "and", LastName: "you", Age: 32},
			),
		},

		{
			scenario: "two row groups without ordering",
			input: []parquet.RowGroup{
				newRowGroup(nil, dataType{FirstName: "some", LastName: "one", Age: 30}),
				newRowGroup(nil, dataType{FirstName: "some", LastName: "one else", Age: 31}),
			},
			output: newRowGroup(nil,
				dataType{FirstName: "some", LastName: "one", Age: 30},
				dataType{FirstName: "some", LastName: "one else", Age: 31},
			),
		},

		{
			scenario: "three row groups without ordering",
			input: []parquet.RowGroup{
				newRowGroup(nil, dataType{FirstName: "some", LastName: "one", Age: 30}),
				newRowGroup(nil, dataType{FirstName: "some", LastName: "one else", Age: 31}),
				newRowGroup(nil, dataType{FirstName: "question", LastName: "answer", Age: 42}),
			},
			output: newRowGroup(nil,
				dataType{FirstName: "some", LastName: "one", Age: 30},
				dataType{FirstName: "some", LastName: "one else", Age: 31},
				dataType{FirstName: "question", LastName: "answer", Age: 42},
			),
		},

		{
			scenario: "row groups sorted by ascending last name",
			options: []parquet.RowGroupOption{
				parquet.SortingColumns(
					parquet.Ascending("LastName"),
				),
			},
			input: []parquet.RowGroup{
				newRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingColumns(
							parquet.Ascending("LastName"),
						),
					},
					dataType{FirstName: "Han", LastName: "Solo"},
					dataType{FirstName: "Luke", LastName: "Skywalker"},
				),
				newRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingColumns(
							parquet.Ascending("LastName"),
						),
					},
					dataType{FirstName: "Obiwan", LastName: "Kenobi"},
				),
			},
			output: newRowGroup(nil,
				dataType{FirstName: "Obiwan", LastName: "Kenobi"},
				dataType{FirstName: "Luke", LastName: "Skywalker"},
				dataType{FirstName: "Han", LastName: "Solo"},
			),
		},

		{
			scenario: "row groups sorted by descending last name",
			options: []parquet.RowGroupOption{
				parquet.SortingColumns(
					parquet.Descending("LastName"),
				),
			},
			input: []parquet.RowGroup{
				newRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingColumns(
							parquet.Descending("LastName"),
						),
					},
					dataType{FirstName: "Han", LastName: "Solo"},
					dataType{FirstName: "Luke", LastName: "Skywalker"},
				),
				newRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingColumns(
							parquet.Descending("LastName"),
						),
					},
					dataType{FirstName: "Obiwan", LastName: "Kenobi"},
				),
			},
			output: newRowGroup(nil,
				dataType{FirstName: "Han", LastName: "Solo"},
				dataType{FirstName: "Luke", LastName: "Skywalker"},
				dataType{FirstName: "Obiwan", LastName: "Kenobi"},
			),
		},

		{
			scenario: "row groups sorted by ascending last and first name",
			options: []parquet.RowGroupOption{
				parquet.SortingColumns(
					parquet.Ascending("LastName"),
					parquet.Ascending("FirstName"),
				),
			},
			input: []parquet.RowGroup{
				newRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingColumns(
							parquet.Ascending("LastName"),
							parquet.Ascending("FirstName"),
						),
					},
					dataType{FirstName: "Luke", LastName: "Skywalker"},
					dataType{FirstName: "Han", LastName: "Solo"},
				),
				newRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingColumns(
							parquet.Ascending("LastName"),
							parquet.Ascending("FirstName"),
						),
					},
					dataType{FirstName: "Obiwan", LastName: "Kenobi"},
					dataType{FirstName: "Anakin", LastName: "Skywalker"},
				),
			},
			output: newRowGroup(nil,
				dataType{FirstName: "Obiwan", LastName: "Kenobi"},
				dataType{FirstName: "Anakin", LastName: "Skywalker"},
				dataType{FirstName: "Luke", LastName: "Skywalker"},
				dataType{FirstName: "Han", LastName: "Solo"},
			),
		},

		{
			scenario: "row groups with conversion to a different schema",
			options: []parquet.RowGroupOption{
				parquet.SchemaOf(subDataType{}),
				parquet.SortingColumns(
					parquet.Ascending("LastName"),
				),
			},
			input: []parquet.RowGroup{
				newRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingColumns(
							parquet.Ascending("LastName"),
						),
					},
					dataType{FirstName: "Han", LastName: "Solo"},
					dataType{FirstName: "Luke", LastName: "Skywalker"},
				),
				newRowGroup(
					[]parquet.RowGroupOption{
						parquet.SortingColumns(
							parquet.Ascending("LastName"),
						),
					},
					dataType{FirstName: "Obiwan", LastName: "Kenobi"},
					dataType{FirstName: "Anakin", LastName: "Skywalker"},
				),
			},
			output: newRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
					),
				},
				subDataType{LastName: "Solo"},
				subDataType{LastName: "Skywalker"},
				subDataType{LastName: "Skywalker"},
				subDataType{LastName: "Kenobi"},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			merged, err := parquet.MergeRowGroups(test.input, test.options...)
			if err != nil {
				t.Fatal(err)
			}
			if merged.NumRows() != test.output.NumRows() {
				t.Fatalf("the number of rows mismatch: want=%d got=%d", merged.NumRows(), test.output.NumRows())
			}
			if merged.Schema() != test.output.Schema() {
				t.Fatalf("the row group schemas mismatch:\n%v\n%v", test.output.Schema(), merged.Schema())
			}

			options := []parquet.RowGroupOption{parquet.SchemaOf(dataType{})}
			options = append(options, test.options...)
			// We test two views of the resulting row group: the one originally
			// returned by MergeRowGroups, and one where the merged row group
			// has been copied into a new buffer. The intent is to exercise both
			// the row-by-row read as well as optimized code thams when CopyRows
			// bypasses the ReadRow/WriteRow calls and the row group is written
			// directly to the buffer by calling WriteRowsTo/WriteRowGroup.
			mergedCopy := parquet.NewBuffer(options...)

			if _, err := parquet.CopyRows(mergedCopy, merged.Rows()); err != nil {
				t.Fatal(err)
			}

			for _, merge := range []struct {
				scenario string
				rowGroup parquet.RowGroup
			}{
				{scenario: "self", rowGroup: merged},
				{scenario: "copy", rowGroup: mergedCopy},
			} {
				t.Run(merge.scenario, func(t *testing.T) {
					var expectedRows = test.output.Rows()
					var mergedRows = merge.rowGroup.Rows()
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
							t.Errorf("row at index %d mismatch: want=%+v got=%+v", numRows, row1, row2)
						}

						numRows++
					}

					if numRows != test.output.NumRows() {
						t.Errorf("expected to read %d rows but %d were found", test.output.NumRows(), numRows)
					}
				})
			}
		})
	}
}
