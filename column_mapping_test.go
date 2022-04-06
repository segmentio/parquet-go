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
	//    0 => "attributes.name"
	//    1 => "attributes.value"
	//    2 => "first_name"
	//    3 => "last_name"
	// }
}
