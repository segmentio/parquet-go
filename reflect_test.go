package parquet_test

import (
	"reflect"
	"testing"

	"github.com/segmentio/parquet"
)

type Empty struct{}

type Person struct {
	ID        [16]byte `parquet:"id"`
	FirstName string   `parquet:"firstName"`
	LastName  string   `parquet:"lastName"`
	Age       int      `parquet:"age,optional"`
	Surnames  []string `parquet:"surnames"`
	Objects   []Object `parquet:"objects"`
}

type Object struct {
	Rare bool `parquet:"rare,optional"`
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
  required fixed_len_byte_array id;
  required binary firstName;
  required binary lastName;
  optional int32 age;
  repeated binary surnames;
  repeated group objects {
    optional boolean rare;
  }
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
