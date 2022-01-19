package parquet_test

import (
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestSchemaOf(t *testing.T) {
	tests := []struct {
		value interface{}
		print string
	}{
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
	required float X;
	required float Y;
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
