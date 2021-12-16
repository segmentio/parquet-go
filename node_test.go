package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet"
)

func BenchmarkTraverse(b *testing.B) {
	row := &AddressBook{
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
	}

	schema := parquet.SchemaOf(row)

	for i := 0; i < b.N; i++ {
		schema.Traverse(row, parquet.TraversalFunc(func(columnIndex int, value parquet.Value) error {
			//
			return nil
		}))
	}
}
