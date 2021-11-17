package parquet_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/parquet"
)

func TestRow(t *testing.T) {
	type Person struct {
		FirstName string
		LastName  string
		Age       int     `parquet:",optional"`
		Weight    float64 `parquet:",optional"`
	}

	type Details struct {
		Person *Person
	}

	type Friend struct {
		ID      [16]byte `parquet:",uuid"`
		Details *Details
	}

	type User struct {
		ID      [16]byte `parquet:",uuid"`
		Details *Details
		Friends []Friend `parquet:",list,optional"`
	}

	tests := []struct {
		scenario string
		input    interface{}
		values   [][]parquet.Value
	}{
		{
			scenario: "single field",
			input: struct {
				Name string
			}{Name: "Luke"},
			values: [][]parquet.Value{
				{parquet.ValueOf("Luke")},
			},
		},

		{
			scenario: "multiple fields",
			input: Person{
				FirstName: "Han",
				LastName:  "Solo",
				Age:       42,
				Weight:    81.5,
			},
			values: [][]parquet.Value{
				{parquet.ValueOf(42).Level(0, 1)},
				{parquet.ValueOf("Han")},
				{parquet.ValueOf("Solo")},
				{parquet.ValueOf(81.5).Level(0, 1)},
			},
		},

		{
			scenario: "empty repeated field",
			input: struct {
				Symbols []string
			}{
				Symbols: nil,
			},
			values: [][]parquet.Value{{}},
		},

		{
			scenario: "single repeated field",
			input: struct {
				Symbols []string
			}{
				Symbols: []string{"EUR", "USD", "GBP", "JPY"},
			},
			values: [][]parquet.Value{
				{
					parquet.ValueOf("EUR").Level(0, 1),
					parquet.ValueOf("USD").Level(1, 1),
					parquet.ValueOf("GBP").Level(1, 1),
					parquet.ValueOf("JPY").Level(1, 1),
				},
			},
		},

		{
			scenario: "multiple repeated field",
			input: struct {
				Symbols []string
				Values  []float32
			}{
				Symbols: []string{"EUR", "USD", "GBP", "JPY"},
				Values:  []float32{0.1, 0.2, 0.3, 0.4},
			},
			values: [][]parquet.Value{
				{
					parquet.ValueOf("EUR").Level(0, 1),
					parquet.ValueOf("USD").Level(1, 1),
					parquet.ValueOf("GBP").Level(1, 1),
					parquet.ValueOf("JPY").Level(1, 1),
				},
				{
					parquet.ValueOf(float32(0.1)).Level(0, 1),
					parquet.ValueOf(float32(0.2)).Level(1, 1),
					parquet.ValueOf(float32(0.3)).Level(1, 1),
					parquet.ValueOf(float32(0.4)).Level(1, 1),
				},
			},
		},

		{
			scenario: "top level nil pointer field",
			input: struct {
				Person *Person
			}{
				Person: nil,
			},
			// Here there are four nil values because the Person type has four
			// fields but it is nil.
			values: [][]parquet.Value{
				{parquet.ValueOf(nil).Level(0, 0)},
				{parquet.ValueOf(nil).Level(0, 0)},
				{parquet.ValueOf(nil).Level(0, 0)},
				{parquet.ValueOf(nil).Level(0, 0)},
			},
		},

		{
			scenario: "sub level nil pointer field",
			input: struct {
				User User
			}{
				User: User{
					ID: uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"),
					Details: &Details{
						Person: nil,
					},
				},
			},
			// Here there are four nil values because the Person type has four
			// fields but it is nil.
			values: [][]parquet.Value{
				{parquet.ValueOf(nil).Level(0, 1)},
				{parquet.ValueOf(nil).Level(0, 1)},
				{parquet.ValueOf(nil).Level(0, 1)},
				{parquet.ValueOf(nil).Level(0, 1)},
				{parquet.ValueOf(uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"))},
			},
		},

		{
			scenario: "deeply dested structure",
			input: struct {
				User User
			}{
				User: User{
					ID: uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"),
					Details: &Details{
						Person: &Person{
							FirstName: "Luke",
							LastName:  "Skywalker",
						},
					},
					Friends: []Friend{
						{
							ID: uuid.MustParse("1B76F8D0-82C6-403F-A104-DCDA69207220"),
							Details: &Details{
								Person: &Person{
									FirstName: "Han",
									LastName:  "Solo",
								},
							},
						},

						{
							ID: uuid.MustParse("C43C8852-CCE5-40E6-B0DF-7212A5633346"),
							Details: &Details{
								Person: &Person{
									FirstName: "Leia",
									LastName:  "Skywalker",
								},
							},
						},

						{
							ID: uuid.MustParse("E78642A8-0931-4D5F-918F-24DC8FF445B0"),
							Details: &Details{
								Person: &Person{
									FirstName: "C3PO",
									LastName:  "Droid",
								},
							},
						},
					},
				},
			},

			values: [][]parquet.Value{
				// User.Details
				{parquet.ValueOf(0).Level(0, 3)},
				{parquet.ValueOf("Luke").Level(0, 2)},
				{parquet.ValueOf("Skywalker").Level(0, 2)},
				{parquet.ValueOf(0.0).Level(0, 3)},

				{ // User.Friends.Details.Person.Age
					parquet.ValueOf(0).Level(0, 4),
					parquet.ValueOf(0).Level(1, 4),
					parquet.ValueOf(0).Level(1, 4),
				},

				{ // User.Friends.Details.Person.FirstName
					parquet.ValueOf("Han").Level(0, 4),
					parquet.ValueOf("Leia").Level(1, 4),
					parquet.ValueOf("C3PO").Level(1, 4),
				},

				{ // User.Friends.Details.Person.LastName
					parquet.ValueOf("Solo").Level(0, 4),
					parquet.ValueOf("Skywalker").Level(1, 4),
					parquet.ValueOf("Droid").Level(1, 4),
				},

				{ // User.Friends.Details.Person.Weight
					parquet.ValueOf(0.0).Level(0, 4),
					parquet.ValueOf(0.0).Level(1, 4),
					parquet.ValueOf(0.0).Level(1, 4),
				},

				{ // User.Friends.ID
					parquet.ValueOf(uuid.MustParse("1B76F8D0-82C6-403F-A104-DCDA69207220")).Level(0, 4),
					parquet.ValueOf(uuid.MustParse("1B76F8D0-82C6-403F-A104-DCDA69207220")).Level(1, 4),
					parquet.ValueOf(uuid.MustParse("E78642A8-0931-4D5F-918F-24DC8FF445B0")).Level(1, 4),
				},

				// User.ID
				{parquet.ValueOf(uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"))},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			schema := parquet.SchemaOf(test.input)
			values := make(map[int][]parquet.Value)
			lastColumnIndex := 0

			t.Log(schema)

			row := parquet.NewRow(schema.Construct(reflect.ValueOf(test.input)))
			for row.Next() {
				columnIndex := row.ColumnIndex()
				if columnIndex < lastColumnIndex {
					t.Fatalf("column indexes were presented out of order: %d < %d", columnIndex, lastColumnIndex)
				}
				lastColumnIndex = columnIndex
				fmt.Println(columnIndex, "=>", row.Value())
				values[columnIndex] = append(values[columnIndex], row.Value())
			}

			for columnIndex, expect := range test.values {
				assertEqualValues(t, columnIndex, expect, values[columnIndex])
			}
		})
	}
}

func assertEqualValues(t *testing.T, columnIndex int, want, got []parquet.Value) {
	n := len(want)

	if len(want) != len(got) {
		t.Errorf("wrong number of values in column %d: want=%d got=%d", columnIndex, len(want), len(got))
		if len(want) > len(got) {
			n = len(got)
		}
	}

	for i := 0; i < n; i++ {
		v1, v2 := want[i], got[i]

		if !parquet.Equal(v1, v2) {
			t.Errorf("values at index %d mismatch in column %d: want=%s got=%s", i, columnIndex, v1, v2)
			break
		}

		if v1.RepetitionLevel() != v2.RepetitionLevel() {
			t.Errorf("repetition levels at index %d mismatch in column %d: want=%d got=%d",
				i, columnIndex,
				v1.RepetitionLevel(),
				v2.RepetitionLevel())
			break
		}

		if v1.DefinitionLevel() != v2.DefinitionLevel() {
			t.Errorf("definition levels at index %d mismatch in column %d: want=%d got=%d",
				i, columnIndex,
				v1.DefinitionLevel(),
				v2.DefinitionLevel())
			break
		}
	}
}
