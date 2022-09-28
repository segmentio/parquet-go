package parquet_test

import (
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

type AddressBook3 struct {
	Owner    string     `parquet:"owner,zstd"`
	Contacts []Contact2 `parquet:"contacts"`
}

type Contact2 struct {
	Name         string   `parquet:"name"`
	PhoneNumbers []string `parquet:"phoneNumbers,zstd"`
	Addresses    []string `parquet:"addresses,zstd"`
}

type AddressBook4 struct {
	Owner    string     `parquet:"owner,zstd"`
	Contacts []Contact2 `parquet:"contacts"`
	Extra    string     `parquet:"extra"`
}

type SimpleNumber struct {
	Number *int64 `parquet:"number,optional"`
}

type SimpleContact struct {
	Numbers []SimpleNumber `parquet:"numbers"`
}

type SimpleAddressBook struct {
	Name    string
	Contact SimpleContact
}

type SimpleAddressBook2 struct {
	Name    string
	Contact SimpleContact
	Extra   string
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

	{
		scenario: "handle nested repeated elements during conversion",
		from: AddressBook3{
			Owner: "Julien Le Dem",
			Contacts: []Contact2{
				{
					Name: "Dmitriy Ryaboy",
					PhoneNumbers: []string{
						"555 987 6543",
						"555 123 4567",
					},
					Addresses: []string{},
				},
				{
					Name: "Chris Aniszczyk",
					PhoneNumbers: []string{
						"555 345 8129",
					},
					Addresses: []string{
						"42 Wallaby Way Sydney",
						"1 White House Way",
					},
				},
				{
					Name: "Bob Ross",
					PhoneNumbers: []string{
						"555 198 3628",
					},
					Addresses: []string{
						"::1",
					},
				},
			},
		},
		to: AddressBook4{
			Owner: "Julien Le Dem",
			Contacts: []Contact2{
				{
					Name: "Dmitriy Ryaboy",
					PhoneNumbers: []string{
						"555 987 6543",
						"555 123 4567",
					},
					Addresses: []string{},
				},
				{
					Name: "Chris Aniszczyk",
					PhoneNumbers: []string{
						"555 345 8129",
					},
					Addresses: []string{
						"42 Wallaby Way Sydney",
						"1 White House Way",
					},
				},
				{
					Name: "Bob Ross",
					PhoneNumbers: []string{
						"555 198 3628",
					},
					Addresses: []string{
						"::1",
					},
				},
			},
			Extra: "",
		},
	},

	{
		scenario: "handle nested repeated elements during conversion",
		from: SimpleAddressBook{
			Name: "New Contact",
			Contact: SimpleContact{
				Numbers: []SimpleNumber{
					{
						Number: nil,
					},
					{
						Number: newInt64(1329),
					},
				},
			},
		},
		to: SimpleAddressBook2{
			Name: "New Contact",
			Contact: SimpleContact{
				Numbers: []SimpleNumber{
					{
						Number: nil,
					},
					{
						Number: newInt64(1329),
					},
				},
			},
			Extra: "",
		},
	},
}

func TestConvert(t *testing.T) {
	for _, test := range conversionTests {
		t.Run(test.scenario, func(t *testing.T) {
			to := parquet.SchemaOf(test.to)
			from := parquet.SchemaOf(test.from)

			conv, err := parquet.Convert(to, from)
			if err != nil {
				t.Fatal(err)
			}

			oldRow := from.Deconstruct(nil, test.from)
			row, err := conv.Convert(nil, oldRow)
			if err != nil {
				t.Fatal(err)
			}

			// Helpful debugging info
			// newRow := to.Deconstruct(nil, test.to)
			// fmt.Printf("conv: %+v\n", conv)
			// fmt.Printf("old row: %+v\n", oldRow)
			// fmt.Printf("new row (desired state): %+v\n", newRow)
			// fmt.Printf("new row (converted from old): %+v\n", row)

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

func newInt64(i int64) *int64    { return &i }
func newString(s string) *string { return &s }
