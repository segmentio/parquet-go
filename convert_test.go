package parquet_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/parquet-go"
)

type AddressBook1 struct {
	Owner             string   `parquet:"owner,zstd"`
	OwnerPhoneNumbers []string `parquet:"ownerPhoneNumbers,gzip"`
}

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

type ListOfIDs struct {
	IDs []uint64
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
		scenario: "extra required column from repeated",
		from: struct{ ListOfIDs ListOfIDs }{
			ListOfIDs: ListOfIDs{IDs: []uint64{0, 1, 2}},
		},
		to: struct {
			MainID    uint64
			ListOfIDs ListOfIDs
		}{
			ListOfIDs: ListOfIDs{IDs: []uint64{0, 1, 2}},
		},
	},

	{
		scenario: "extra fields in repeated group",
		from: struct{ Books []AddressBook1 }{
			Books: []AddressBook1{
				{
					Owner:             "me",
					OwnerPhoneNumbers: []string{"123-456-7890", "321-654-0987"},
				},
				{
					Owner:             "you",
					OwnerPhoneNumbers: []string{"000-000-0000"},
				},
			},
		},
		to: struct{ Books []AddressBook2 }{
			Books: []AddressBook2{
				{
					Owner:             "me",
					OwnerPhoneNumbers: []string{"123-456-7890", "321-654-0987"},
					Contacts:          []Contact{},
				},
				{
					Owner:             "you",
					OwnerPhoneNumbers: []string{"000-000-0000"},
					Contacts:          []Contact{},
				},
			},
		},
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
		scenario: "required to optional leaf",
		from:     struct{ Name string }{Name: "Luke"},
		to:       struct{ Name *string }{Name: newString("Luke")},
	},

	{
		scenario: "required to repeated leaf",
		from:     struct{ Name string }{Name: "Luke"},
		to:       struct{ Name []string }{Name: []string{"Luke"}},
	},

	{
		scenario: "optional to required leaf",
		from:     struct{ Name *string }{Name: newString("Luke")},
		to:       struct{ Name string }{Name: "Luke"},
	},

	{
		scenario: "optional to repeated leaf",
		from:     struct{ Name *string }{Name: newString("Luke")},
		to:       struct{ Name []string }{Name: []string{"Luke"}},
	},

	{
		scenario: "optional to repeated leaf (null)",
		from:     struct{ Name *string }{Name: nil},
		to:       struct{ Name []string }{Name: []string{}},
	},

	{
		scenario: "repeated to required leaf",
		from:     struct{ Name []string }{Name: []string{"Luke", "Han", "Leia"}},
		to:       struct{ Name string }{Name: "Luke"},
	},

	{
		scenario: "repeated to optional leaf",
		from:     struct{ Name []string }{Name: []string{"Luke", "Han", "Leia"}},
		to:       struct{ Name *string }{Name: newString("Luke")},
	},

	{
		scenario: "required to optional group",
		from: struct{ Book AddressBook }{
			Book: AddressBook{
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
		to: struct{ Book *AddressBook }{
			Book: &AddressBook{
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
	},

	{
		scenario: "required to optional group (empty)",
		from: struct{ Book AddressBook }{
			Book: AddressBook{},
		},
		to: struct{ Book *AddressBook }{
			Book: &AddressBook{
				OwnerPhoneNumbers: []string{},
				Contacts:          []Contact{},
			},
		},
	},

	{
		scenario: "optional to required group (null)",
		from: struct{ Book *AddressBook }{
			Book: nil,
		},
		to: struct{ Book AddressBook }{
			Book: AddressBook{
				OwnerPhoneNumbers: []string{},
				Contacts:          []Contact{},
			},
		},
	},

	{
		scenario: "optional to repeated group (null)",
		from:     struct{ Book *AddressBook }{Book: nil},
		to:       struct{ Book []AddressBook }{Book: []AddressBook{}},
	},

	{
		scenario: "optional to repeated optional group (null)",
		from:     struct{ Book *AddressBook }{Book: nil},
		to:       struct{ Book []*AddressBook }{Book: []*AddressBook{}},
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

			row := from.Deconstruct(nil, test.from)
			rowbuf := []parquet.Row{row}
			n, err := conv.Convert(rowbuf)
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Errorf("wrong number of rows got converted: want=1 got=%d", n)
			}
			row = rowbuf[0]

			// row.Range(func(i int, v []parquet.Value) bool {
			// 	t.Logf("%d. %+v\n", i, v)
			// 	return true
			// })

			//t.Logf("%+v\n", row)

			value := reflect.New(reflect.TypeOf(test.to))
			if err := to.Reconstruct(value.Interface(), row); err != nil {
				t.Fatal(err)
			}

			value = value.Elem()
			if !reflect.DeepEqual(value.Interface(), test.to) {
				t.Errorf("converted value mismatch:\nwant = %#v\ngot  = %#v", test.to, value.Interface())
			}
		})
	}
}

func newInt64(i int64) *int64    { return &i }
func newString(s string) *string { return &s }

func TestConvertTimestamp(t *testing.T) {
	now := time.Unix(42, 0)
	ms := now.UnixMilli()
	us := now.UnixMicro()
	ns := now.UnixNano()

	msType := parquet.Timestamp(parquet.Millisecond).Type()
	msVal := parquet.ValueOf(ms)
	if msVal.Int64() != ms {
		t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", ms, msVal.Int64())
	}

	usType := parquet.Timestamp(parquet.Microsecond).Type()
	usVal := parquet.ValueOf(us)
	if usVal.Int64() != us {
		t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", us, usVal.Int64())
	}

	nsType := parquet.Timestamp(parquet.Nanosecond).Type()
	nsVal := parquet.ValueOf(ns)
	if nsVal.Int64() != ns {
		t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", ns, nsVal.Int64())
	}

	var timestampConversionTests = [...]struct {
		scenario  string
		fromType  parquet.Type
		fromValue parquet.Value
		toType    parquet.Type
		expected  int64
	}{
		{
			scenario:  "micros to nanos",
			fromType:  usType,
			fromValue: usVal,
			toType:    nsType,
			expected:  ns,
		},
		{
			scenario:  "millis to nanos",
			fromType:  msType,
			fromValue: msVal,
			toType:    nsType,
			expected:  ns,
		},
		{
			scenario:  "nanos to micros",
			fromType:  nsType,
			fromValue: nsVal,
			toType:    usType,
			expected:  us,
		},
		{
			scenario:  "nanos to nanos",
			fromType:  nsType,
			fromValue: nsVal,
			toType:    nsType,
			expected:  ns,
		},
		{
			scenario:  "int64 to nanos",
			fromType:  parquet.Int64Type,
			fromValue: nsVal,
			toType:    nsType,
			expected:  ns,
		},
		{
			scenario:  "int64 to int64",
			fromType:  parquet.Int64Type,
			fromValue: nsVal,
			toType:    parquet.Int64Type,
			expected:  ns,
		},
	}

	for _, test := range timestampConversionTests {
		t.Run(test.scenario, func(t *testing.T) {
			a, err := test.toType.ConvertValue(test.fromValue, test.fromType)
			if err != nil {
				t.Fatal(err)
			}
			if a.Int64() != test.expected {
				t.Errorf("converted value mismatch:\nwant = %+v\ngot  = %+v", test.expected, a.Int64())
			}
		})
	}
}
