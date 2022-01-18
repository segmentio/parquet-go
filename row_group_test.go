package parquet_test

import (
	"bytes"
	"sort"
	"testing"

	"github.com/segmentio/parquet"
)

func sortedRowGroup(options []parquet.RowGroupOption, rows ...interface{}) parquet.RowGroup {
	buf := parquet.NewBuffer(options...)
	for _, row := range rows {
		buf.Write(row)
	}
	sort.Stable(buf)
	return buf
}

type Person struct {
	FirstName string
	LastName  string
	Age       int
}

type LastNameOnly struct {
	LastName string
}

var mergeRowGroupsTests = [...]struct {
	scenario string
	options  []parquet.RowGroupOption
	input    []parquet.RowGroup
	output   parquet.RowGroup
}{
	{
		scenario: "no row groups",
		options: []parquet.RowGroupOption{
			parquet.SchemaOf(Person{}),
		},
		output: sortedRowGroup(
			[]parquet.RowGroupOption{
				parquet.SchemaOf(Person{}),
			},
		),
	},

	{
		scenario: "a single row group",
		input: []parquet.RowGroup{
			sortedRowGroup(nil,
				Person{FirstName: "some", LastName: "one", Age: 30},
				Person{FirstName: "some", LastName: "one else", Age: 31},
				Person{FirstName: "and", LastName: "you", Age: 32},
			),
		},
		output: sortedRowGroup(nil,
			Person{FirstName: "some", LastName: "one", Age: 30},
			Person{FirstName: "some", LastName: "one else", Age: 31},
			Person{FirstName: "and", LastName: "you", Age: 32},
		),
	},

	{
		scenario: "two row groups without ordering",
		input: []parquet.RowGroup{
			sortedRowGroup(nil, Person{FirstName: "some", LastName: "one", Age: 30}),
			sortedRowGroup(nil, Person{FirstName: "some", LastName: "one else", Age: 31}),
		},
		output: sortedRowGroup(nil,
			Person{FirstName: "some", LastName: "one", Age: 30},
			Person{FirstName: "some", LastName: "one else", Age: 31},
		),
	},

	{
		scenario: "three row groups without ordering",
		input: []parquet.RowGroup{
			sortedRowGroup(nil, Person{FirstName: "some", LastName: "one", Age: 30}),
			sortedRowGroup(nil, Person{FirstName: "some", LastName: "one else", Age: 31}),
			sortedRowGroup(nil, Person{FirstName: "question", LastName: "answer", Age: 42}),
		},
		output: sortedRowGroup(nil,
			Person{FirstName: "some", LastName: "one", Age: 30},
			Person{FirstName: "some", LastName: "one else", Age: 31},
			Person{FirstName: "question", LastName: "answer", Age: 42},
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
			sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
					),
				},
				Person{FirstName: "Han", LastName: "Solo"},
				Person{FirstName: "Luke", LastName: "Skywalker"},
			),
			sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
					),
				},
				Person{FirstName: "Obiwan", LastName: "Kenobi"},
			),
		},
		output: sortedRowGroup(nil,
			Person{FirstName: "Obiwan", LastName: "Kenobi"},
			Person{FirstName: "Luke", LastName: "Skywalker"},
			Person{FirstName: "Han", LastName: "Solo"},
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
			sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Descending("LastName"),
					),
				},
				Person{FirstName: "Han", LastName: "Solo"},
				Person{FirstName: "Luke", LastName: "Skywalker"},
			),
			sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Descending("LastName"),
					),
				},
				Person{FirstName: "Obiwan", LastName: "Kenobi"},
			),
		},
		output: sortedRowGroup(nil,
			Person{FirstName: "Han", LastName: "Solo"},
			Person{FirstName: "Luke", LastName: "Skywalker"},
			Person{FirstName: "Obiwan", LastName: "Kenobi"},
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
			sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
						parquet.Ascending("FirstName"),
					),
				},
				Person{FirstName: "Luke", LastName: "Skywalker"},
				Person{FirstName: "Han", LastName: "Solo"},
			),
			sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
						parquet.Ascending("FirstName"),
					),
				},
				Person{FirstName: "Obiwan", LastName: "Kenobi"},
				Person{FirstName: "Anakin", LastName: "Skywalker"},
			),
		},
		output: sortedRowGroup(nil,
			Person{FirstName: "Obiwan", LastName: "Kenobi"},
			Person{FirstName: "Anakin", LastName: "Skywalker"},
			Person{FirstName: "Luke", LastName: "Skywalker"},
			Person{FirstName: "Han", LastName: "Solo"},
		),
	},

	{
		scenario: "row groups with conversion to a different schema",
		options: []parquet.RowGroupOption{
			parquet.SchemaOf(LastNameOnly{}),
			parquet.SortingColumns(
				parquet.Ascending("LastName"),
			),
		},
		input: []parquet.RowGroup{
			sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
					),
				},
				Person{FirstName: "Han", LastName: "Solo"},
				Person{FirstName: "Luke", LastName: "Skywalker"},
			),
			sortedRowGroup(
				[]parquet.RowGroupOption{
					parquet.SortingColumns(
						parquet.Ascending("LastName"),
					),
				},
				Person{FirstName: "Obiwan", LastName: "Kenobi"},
				Person{FirstName: "Anakin", LastName: "Skywalker"},
			),
		},
		output: sortedRowGroup(
			[]parquet.RowGroupOption{
				parquet.SortingColumns(
					parquet.Ascending("LastName"),
				),
			},
			LastNameOnly{LastName: "Solo"},
			LastNameOnly{LastName: "Skywalker"},
			LastNameOnly{LastName: "Skywalker"},
			LastNameOnly{LastName: "Kenobi"},
		),
	},
}

func selfRowGroup(rowGroup parquet.RowGroup) parquet.RowGroup {
	return rowGroup
}

func fileRowGroup(rowGroup parquet.RowGroup) parquet.RowGroup {
	buffer := new(bytes.Buffer)
	writer := parquet.NewWriter(buffer)
	if _, err := writer.WriteRowGroup(rowGroup); err != nil {
		panic(err)
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	reader := bytes.NewReader(buffer.Bytes())
	f, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		panic(err)
	}
	return f.RowGroup(0)
}

func TestMergeRowGroups(t *testing.T) {
	for _, adapter := range []struct {
		scenario string
		function func(parquet.RowGroup) parquet.RowGroup
	}{
		{scenario: "buffer", function: selfRowGroup},
		{scenario: "file", function: fileRowGroup},
	} {
		t.Run(adapter.scenario, func(t *testing.T) {
			for _, test := range mergeRowGroupsTests {
				t.Run(test.scenario, func(t *testing.T) {
					input := make([]parquet.RowGroup, len(test.input))
					for i := range test.input {
						input[i] = adapter.function(test.input[i])
					}

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

					options := []parquet.RowGroupOption{parquet.SchemaOf(Person{})}
					options = append(options, test.options...)
					// We test two views of the resulting row group: the one originally
					// returned by MergeRowGroups, and one where the merged row group
					// has been copied into a new buffer. The intent is to exercise both
					// the row-by-row read as well as optimized code paths when CopyRows
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
									t.Fatalf("errors mismatched while comparing row %+v: want=%v got=%v", row1, err1, err2)
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
		})
	}
}
