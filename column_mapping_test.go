package parquet_test

import (
	"fmt"

	"github.com/segmentio/parquet-go"
)

func ExampleColumnMapping() {
	schema := parquet.SchemaOf(struct {
		FirstName  string `parquet:"first_name"`
		LastName   string `parquet:"last_name"`
		Attributes []struct {
			Name  string `parquet:"name"`
			Value string `parquet:"value"`
		} `parquet:"attributes"`
	}{})

	mapping := parquet.ColumnMappingOf(schema)
	fmt.Println(mapping)

	// Output:
	// {
	//    0 => "first_name"
	//    1 => "last_name"
	//    2 => "attributes.name"
	//    3 => "attributes.value"
	// }
}
