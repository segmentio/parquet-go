package parquet_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/parquet"
)

type nestedListsLevel1 struct {
	Level2 []string `parquet:"level2"`
}

type nestedLists struct {
	Level1 []nestedListsLevel1 `parquet:"level1"`
}

type Contact struct {
	Name        string `parquet:"name"`
	PhoneNumber string `parquet:"phoneNumber,optional"`
}

type AddressBook struct {
	Owner             string    `parquet:"owner"`
	OwnerPhoneNumbers []string  `parquet:"ownerPhoneNumbers"`
	Contacts          []Contact `parquet:"contacts"`
}

func TestSchemaOf(t *testing.T) {
	tests := []struct {
		value interface{}
		print string
	}{
		{
			value: struct{}{},
			print: `message {}`,
		},

		{
			value: new(struct{ Name string }),
			print: `message {
	required binary Name (STRING);
}`,
		},

		{
			value: new(struct {
				X int
				Y int
			}),
			print: `message {
	required int64 X (INT(64,true));
	required int64 Y (INT(64,true));
}`,
		},

		{
			value: new(struct {
				X float32
				Y float32
			}),
			print: `message {
	required float X (DECIMAL(0,9));
	required float Y (DECIMAL(0,9));
}`,
		},

		{
			value: new(struct {
				Inner struct {
					FirstName string `parquet:"first_name"`
					LastName  string `parquet:"last_name"`
				} `parquet:"inner,optional"`
			}),
			print: `message {
	optional group inner {
		required binary first_name (STRING);
		required binary last_name (STRING);
	}
}`,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			schema := parquet.SchemaOf(test.value)

			if s := schema.String(); s != test.print {
				t.Errorf("\nexpected:\n\n%s\n\nfound:\n\n%s\n", test.print, s)
			}
		})
	}
}

func TestSchemaTraversal(t *testing.T) {
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

	type List2 struct {
		Value string `parquet:",optional"`
	}

	type List1 struct {
		List2 []List2 `parquet:",list"`
	}

	type List0 struct {
		List1 []List1 `parquet:",list"`
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
				0: {parquet.ValueOf("Luke")},
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
				0: {parquet.ValueOf(42).Level(0, 1)},
				1: {parquet.ValueOf("Han")},
				2: {parquet.ValueOf("Solo")},
				3: {parquet.ValueOf(81.5).Level(0, 1)},
			},
		},

		{
			scenario: "empty repeated field",
			input: struct {
				Symbols []string
			}{
				Symbols: nil,
			},
			values: [][]parquet.Value{
				0: {parquet.ValueOf(nil).Level(0, 0)},
			},
		},

		{
			scenario: "single repeated field",
			input: struct {
				Symbols []string
			}{
				Symbols: []string{"EUR", "USD", "GBP", "JPY"},
			},
			values: [][]parquet.Value{
				0: {
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
				0: {
					parquet.ValueOf("EUR").Level(0, 1),
					parquet.ValueOf("USD").Level(1, 1),
					parquet.ValueOf("GBP").Level(1, 1),
					parquet.ValueOf("JPY").Level(1, 1),
				},
				1: {
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
				0: {parquet.ValueOf(nil).Level(0, 0)},
				1: {parquet.ValueOf(nil).Level(0, 0)},
				2: {parquet.ValueOf(nil).Level(0, 0)},
				3: {parquet.ValueOf(nil).Level(0, 0)},
			},
		},

		{
			scenario: "sub level nil pointer field",
			input: User{
				ID: uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"),
				Details: &Details{
					Person: nil,
				},
			},
			// Here there are four nil values because the Person type has four
			// fields but it is nil.
			values: [][]parquet.Value{
				// User.Details.Person
				0: {parquet.ValueOf(nil).Level(0, 1)},
				1: {parquet.ValueOf(nil).Level(0, 1)},
				2: {parquet.ValueOf(nil).Level(0, 1)},
				3: {parquet.ValueOf(nil).Level(0, 1)},
				// User.Friends.Details.Person
				4: {parquet.ValueOf(nil).Level(0, 0)},
				5: {parquet.ValueOf(nil).Level(0, 0)},
				6: {parquet.ValueOf(nil).Level(0, 0)},
				7: {parquet.ValueOf(nil).Level(0, 0)},
				// User.Friends.ID
				8: {parquet.ValueOf(nil).Level(0, 0)},
				// User.ID
				9: {parquet.ValueOf(uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"))},
			},
		},

		{
			scenario: "deeply nested structure",
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
				0: {parquet.ValueOf(nil).Level(0, 2)},
				1: {parquet.ValueOf("Luke").Level(0, 2)},
				2: {parquet.ValueOf("Skywalker").Level(0, 2)},
				3: {parquet.ValueOf(nil).Level(0, 2)},

				4: { // User.Friends.Details.Person.Age
					parquet.ValueOf(nil).Level(0, 4),
					parquet.ValueOf(nil).Level(1, 4),
					parquet.ValueOf(nil).Level(1, 4),
				},

				5: { // User.Friends.Details.Person.FirstName
					parquet.ValueOf("Han").Level(0, 4),
					parquet.ValueOf("Leia").Level(1, 4),
					parquet.ValueOf("C3PO").Level(1, 4),
				},

				6: { // User.Friends.Details.Person.LastName
					parquet.ValueOf("Solo").Level(0, 4),
					parquet.ValueOf("Skywalker").Level(1, 4),
					parquet.ValueOf("Droid").Level(1, 4),
				},

				7: { // User.Friends.Details.Person.Weight
					parquet.ValueOf(nil).Level(0, 4),
					parquet.ValueOf(nil).Level(1, 4),
					parquet.ValueOf(nil).Level(1, 4),
				},

				8: { // User.Friends.ID
					parquet.ValueOf(uuid.MustParse("1B76F8D0-82C6-403F-A104-DCDA69207220")).Level(0, 2),
					parquet.ValueOf(uuid.MustParse("C43C8852-CCE5-40E6-B0DF-7212A5633346")).Level(1, 2),
					parquet.ValueOf(uuid.MustParse("E78642A8-0931-4D5F-918F-24DC8FF445B0")).Level(1, 2),
				},

				// User.ID
				9: {parquet.ValueOf(uuid.MustParse("A65B576D-9299-4769-9D93-04BE0583F027"))},
			},
		},

		{
			scenario: "multiple repeated levels",
			input: List0{
				List1: []List1{
					{List2: []List2{{Value: "A"}, {Value: "B"}}},
					{List2: []List2{}},
					{List2: []List2{{Value: "C"}}},
				},
			},
			values: [][]parquet.Value{
				{
					parquet.ValueOf("A").Level(0, 3),
					parquet.ValueOf("B").Level(2, 3),
					parquet.ValueOf(nil).Level(1, 2),
					parquet.ValueOf("C").Level(1, 3),
				},
			},
		},

		// https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet

		// message nestedLists {
		//   repeated group level1 {
		//     repeated string level2;
		//   }
		// }
		// ---
		// {
		//   level1: {
		//     level2: a
		//     level2: b
		//     level2: c
		//   },
		//   level1: {
		//     level2: d
		//     level2: e
		//     level2: f
		//     level2: g
		//   }
		// }
		//
		{
			scenario: "twitter blog example 1",
			input: nestedLists{
				Level1: []nestedListsLevel1{
					{Level2: []string{"a", "b", "c"}},
					{Level2: []string{"d", "e", "f", "g"}},
				},
			},
			values: [][]parquet.Value{
				0: {
					parquet.ValueOf("a").Level(0, 2),
					parquet.ValueOf("b").Level(2, 2),
					parquet.ValueOf("c").Level(2, 2),
					parquet.ValueOf("d").Level(1, 2),
					parquet.ValueOf("e").Level(2, 2),
					parquet.ValueOf("f").Level(2, 2),
					parquet.ValueOf("g").Level(2, 2),
				},
			},
		},

		// message nestedLists {
		//   repeated group level1 {
		//     repeated string level2;
		//   }
		// }
		// ---
		// {
		//   level1: {
		//     level2: h
		//   },
		//   level1: {
		//     level2: i
		//     level2: j
		//   }
		// }
		//
		{
			scenario: "twitter blog example 2",
			input: nestedLists{
				Level1: []nestedListsLevel1{
					{Level2: []string{"h"}},
					{Level2: []string{"i", "j"}},
				},
			},
			values: [][]parquet.Value{
				0: {
					parquet.ValueOf("h").Level(0, 2),
					parquet.ValueOf("i").Level(1, 2),
					parquet.ValueOf("j").Level(2, 2),
				},
			},
		},

		// message AddressBook {
		//   required string owner;
		//   repeated string ownerPhoneNumbers;
		//   repeated group contacts {
		//     required string name;
		//     optional string phoneNumber;
		//   }
		// }
		// ---
		// AddressBook {
		//   owner: "Julien Le Dem",
		//   ownerPhoneNumbers: "555 123 4567",
		//   ownerPhoneNumbers: "555 666 1337",
		//   contacts: {
		//     name: "Dmitriy Ryaboy",
		//     phoneNumber: "555 987 6543",
		//   },
		//   contacts: {
		//     name: "Chris Aniszczyk"
		//   }
		// }
		{
			scenario: "twitter blog example 3",
			input: AddressBook{
				Owner: "Julien Le Dem",
				OwnerPhoneNumbers: []string{
					"555 123 4567",
					"555 666 1337",
				},
				Contacts: []Contact{
					{
						Name:        "Dmitriy Ryaboy",
						PhoneNumber: "555 987 6543",
					},
					{
						Name: "Chris Aniszczyk",
					},
				},
			},
			values: [][]parquet.Value{
				0: { // AddressBook.contacts.name
					parquet.ValueOf("Dmitriy Ryaboy").Level(0, 1),
					parquet.ValueOf("Chris Aniszczyk").Level(1, 1),
				},
				1: { // AddressBook.contacts.phoneNumber
					parquet.ValueOf("555 987 6543").Level(0, 2),
					parquet.ValueOf(nil).Level(1, 1),
				},
				2: { // AddressBook.owner
					parquet.ValueOf("Julien Le Dem").Level(0, 0),
				},
				3: { // AddressBook.ownerPhoneNumbers
					parquet.ValueOf("555 123 4567").Level(0, 1),
					parquet.ValueOf("555 666 1337").Level(1, 1),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			schema := parquet.SchemaOf(test.input)
			values := make(map[int][]parquet.Value)

			t.Logf("\n%s\n", schema)

			methods := []struct {
				scenario string
				traverse func(interface{}, parquet.Traversal) error
			}{
				{
					scenario: "generic",
					traverse: func(value interface{}, traversal parquet.Traversal) error {
						return parquet.Traverse(schema, value, traversal)
					},
				},
				{
					scenario: "optimized",
					traverse: schema.Traverse,
				},
			}

			for _, method := range methods {
				t.Run(method.scenario, func(t *testing.T) {
					for columnIndex := range values {
						delete(values, columnIndex)
					}

					err := method.traverse(test.input, parquet.TraversalFunc(func(columnIndex int, value parquet.Value) error {
						values[columnIndex] = append(values[columnIndex], value)
						return nil
					}))
					if err != nil {
						t.Fatal(err)
					}

					for columnIndex, expect := range test.values {
						assertEqualValues(t, columnIndex, expect, values[columnIndex])
						delete(values, columnIndex)
					}

					for columnIndex, unexpected := range values {
						t.Errorf("unexpected column index %d found with %d values in it", columnIndex, len(unexpected))
					}
				})
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
		}

		if v1.RepetitionLevel() != v2.RepetitionLevel() {
			t.Errorf("repetition levels at index %d mismatch in column %d: want=%d got=%d",
				i, columnIndex,
				v1.RepetitionLevel(),
				v2.RepetitionLevel())
		}

		if v1.DefinitionLevel() != v2.DefinitionLevel() {
			t.Errorf("definition levels at index %d mismatch in column %d: want=%d got=%d",
				i, columnIndex,
				v1.DefinitionLevel(),
				v2.DefinitionLevel())
		}
	}
}
