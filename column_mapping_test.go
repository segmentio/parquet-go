package parquet_test

import (
	"fmt"
	"strings"

	"github.com/segmentio/parquet-go"
)

func ExampleSchema_Lookup() {
	schema := parquet.SchemaOf(struct {
		FirstName  string `parquet:"first_name"`
		LastName   string `parquet:"last_name"`
		Attributes []struct {
			Name  string `parquet:"name"`
			Value string `parquet:"value"`
		} `parquet:"attributes"`
	}{})

	for _, columnPath := range schema.Columns() {
		columnIndex, _ := schema.Lookup(columnPath...)
		fmt.Printf("%d => %q\n", columnIndex, strings.Join(columnPath, "."))
	}

	// Output:
	// 0 => "first_name"
	// 1 => "last_name"
	// 2 => "attributes.name"
	// 3 => "attributes.value"
}
