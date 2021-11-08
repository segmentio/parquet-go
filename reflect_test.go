package parquet_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/parquet"
)

type Empty struct{}

type Person struct {
	ID        [16]byte  `parquet:"id,uuid"`
	FirstName string    `parquet:"first_name"`
	LastName  string    `parquet:"last_name"`
	Age       int       `parquet:"age,optional"`
	Surnames  []string  `parquet:"surnames,enum"`
	Objects   []Object  `parquet:"objects,optional,list"`
	Birthday  time.Time `parquet:"birthday"`
}

type Object struct {
	Rare   bool    `parquet:"rare,optional"`
	Angle  float32 `parquet:"angle"`
	Weight float64 `parquet:"weight"`
}

func TestSchemaOf(t *testing.T) {
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
  required fixed_len_byte_array id (UUID);
  required binary first_name (UTF8);
  required binary last_name (UTF8);
  optional int32 age (INT(32, true));
  repeated binary surnames (ENUM);
  optional group objects (LIST) {
    repeated group list {
      required group element {
        optional boolean rare;
        required float angle;
        required double weight;
      }
    }
  }
  required int64 birthday (TIMESTAMP(isAdjustedToUTC=true, unit=NANOS));
}`,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			s := parquet.Format(parquet.SchemaOf(reflect.TypeOf(test.gotype)))
			if s != test.format {
				t.Errorf("\nexpected:\n%s\nfound:\n%s", test.format, s)
			}
		})
	}
}
