package parquet_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go"
)

type AddressBook2 struct {
	Owner             string    `parquet:"owner,zstd"`
	OwnerPhoneNumbers []string  `parquet:"ownerPhoneNumbers,gzip"`
	Contacts          []Contact `parquet:"contacts"`
	Extra             string    `parquet:"extra"`
}

var conversionTests = [...]struct {
	scenario string
	from     interface{}
	to       interface{}
}{
	{
		scenario: "convert between rows which have the same schema",
		from: AddressBook{
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
		to: AddressBook{
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
	},

	{
		scenario: "missing column",
		from:     struct{ FirstName, LastName string }{FirstName: "Luke", LastName: "Skywalker"},
		to:       struct{ LastName string }{LastName: "Skywalker"},
	},

	{
		scenario: "missing optional column",
		from: struct {
			FirstName *string
			LastName  string
		}{FirstName: newString("Luke"), LastName: "Skywalker"},
		to: struct{ LastName string }{LastName: "Skywalker"},
	},

	{
		scenario: "missing repeated column",
		from: struct {
			ID    uint64
			Names []string
		}{ID: 42, Names: []string{"me", "myself", "I"}},
		to: struct{ ID uint64 }{ID: 42},
	},

	{
		scenario: "extra column",
		from:     struct{ LastName string }{LastName: "Skywalker"},
		to:       struct{ FirstName, LastName string }{LastName: "Skywalker"},
	},

	{
		scenario: "extra optional column",
		from:     struct{ ID uint64 }{ID: 2},
		to: struct {
			ID      uint64
			Details *struct{ FirstName, LastName string }
		}{ID: 2, Details: nil},
	},

	{
		scenario: "extra repeated column",
		from:     struct{ ID uint64 }{ID: 1},
		to: struct {
			ID    uint64
			Names []string
		}{ID: 1, Names: []string{}},
	},

	{
		scenario: "extra column on complex struct",
		from: AddressBook{
			Owner:             "Julien Le Dem",
			OwnerPhoneNumbers: []string{},
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
		to: AddressBook2{
			Owner:             "Julien Le Dem",
			OwnerPhoneNumbers: []string{},
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
	},
}

// func TestConvert(t *testing.T) {
// 	for _, test := range conversionTests {
// 		t.Run(test.scenario, func(t *testing.T) {
// 			to := parquet.SchemaOf(test.to)
// 			from := parquet.SchemaOf(test.from)

// 			conv, err := parquet.Convert(to, from)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			newRow := to.Deconstruct(nil, test.to)

// 			oldRow := from.Deconstruct(nil, test.from)
// 			row, err := conv.Convert(nil, oldRow)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			fmt.Printf("conv: %+v\n", conv)
// 			fmt.Printf("old row: %+v\n", oldRow)
// 			fmt.Printf("new row (unconverted): %+v\n", newRow)
// 			fmt.Printf("new row (converted): %+v\n", row)
// 			// fmt.Printf("to Schema: %+v\n", to)

// 			value := reflect.New(reflect.TypeOf(test.to))
// 			if err := to.Reconstruct(value.Interface(), row); err != nil {
// 				t.Fatal(err)
// 			}

// 			value = value.Elem()
// 			if !reflect.DeepEqual(value.Interface(), test.to) {
// 				t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", test.to, value.Interface())
// 			}
// 		})
// 	}
// }

func TestConvert2(t *testing.T) {
	var testCases = [...]struct {
		scenario string
		from     interface{}
		to       interface{}
	}{
		{
			scenario: "missing column",
			from:     struct{ FirstName, LastName string }{FirstName: "Luke", LastName: "Skywalker"},
			to:       struct{ LastName string }{LastName: "Skywalker"},
		},

		{
			scenario: "missing optional column",
			from: struct {
				FirstName *string
				LastName  string
			}{FirstName: newString("Luke"), LastName: "Skywalker"},
			to: struct{ LastName string }{LastName: "Skywalker"},
		},

		{
			scenario: "missing repeated column",
			from: struct {
				ID    uint64
				Names []string
			}{ID: 42, Names: []string{"me", "myself", "I"}},
			to: struct{ ID uint64 }{ID: 42},
		},

		{
			scenario: "extra column",
			from:     struct{ LastName string }{LastName: "Skywalker"},
			to:       struct{ FirstName, LastName string }{LastName: "Skywalker"},
		},

		{
			scenario: "extra optional column",
			from:     struct{ ID uint64 }{ID: 2},
			to: struct {
				ID      uint64
				Details *struct{ FirstName, LastName string }
			}{ID: 2, Details: nil},
		},

		{
			scenario: "extra repeated column",
			from:     struct{ ID uint64 }{ID: 1},
			to: struct {
				ID    uint64
				Names []string
			}{ID: 1, Names: []string{}},
		},

		{
			scenario: "extra column on complex struct",
			from: AddressBook{
				Owner:             "Julien Le Dem",
				OwnerPhoneNumbers: []string{},
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
			to: AddressBook2{
				Owner:             "Julien Le Dem",
				OwnerPhoneNumbers: []string{},
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
		},
	}

	for _, test := range testCases {
		t.Run(test.scenario, func(t *testing.T) {
			to := parquet.SchemaOf(test.to)
			from := parquet.SchemaOf(test.from)

			conv, err := parquet.Convert(to, from)
			if err != nil {
				t.Fatal(err)
			}

			newRow := to.Deconstruct(nil, test.to)

			oldRow := from.Deconstruct(nil, test.from)
			row, err := conv.Convert2(nil, oldRow)
			if err != nil {
				t.Fatal(err)
			}

			fmt.Printf("conv: %+v\n", conv)
			fmt.Printf("old row: %+v\n", oldRow)
			fmt.Printf("new row (unconverted): %+v\n", newRow)
			fmt.Printf("new row (converted): %+v\n", row)
			// fmt.Printf("to Schema: %+v\n", to)

			value := reflect.New(reflect.TypeOf(test.to))
			if err := to.Reconstruct(value.Interface(), row); err != nil {
				t.Fatal(err)
			}

			value = value.Elem()
			if !reflect.DeepEqual(value.Interface(), test.to) {
				t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", test.to, value.Interface())
			}
		})
	}
}

func newString(s string) *string { return &s }
