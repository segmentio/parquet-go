package parquet_test

import (
	"reflect"
	"testing"

	"github.com/segmentio/parquet"
)

type Empty struct{}

type Person struct {
	FirstName string `parquet:"firstName"`
	LastName  string `parquet:"lastName"`
}

func TestMessageTypeOf(t *testing.T) {
	tests := []struct {
		gotype interface{}
		format string
	}{
		{
			gotype: Empty{},
			format: `message Empty {}`,
		},

		{
			gotype: Person{},
			format: `message Person {
  required binary firstName;
  required binary lastName;
}`,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			s := parquet.Format(parquet.MessageTypeOf(reflect.TypeOf(test.gotype)))
			if s != test.format {
				t.Errorf("\nexpected:\n%s\nfound:\n%s", test.format, s)
			}
		})
	}
}
