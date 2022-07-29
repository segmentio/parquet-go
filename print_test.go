package parquet_test

import (
	"strings"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestPrintSchema(t *testing.T) {
	tests := []struct {
		node  parquet.Node
		print string
	}{
		{
			node: parquet.NewGroup(
				parquet.GroupNodes{"on": parquet.Leaf(parquet.BooleanType)}),
			print: `message Test {
	required boolean on;
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"name": parquet.String()}),
			print: `message Test {
	required binary name (STRING);
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"uuid": parquet.UUID()}),
			print: `message Test {
	required fixed_len_byte_array(16) uuid (UUID);
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"enum": parquet.Enum()}),
			print: `message Test {
	required binary enum (ENUM);
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"json": parquet.JSON()}),
			print: `message Test {
	required binary json (JSON);
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"bson": parquet.BSON()}),
			print: `message Test {
	required binary bson (BSON);
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"name": parquet.Optional(parquet.String())}),
			print: `message Test {
	optional binary name (STRING);
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"name": parquet.Repeated(parquet.String())}),
			print: `message Test {
	repeated binary name (STRING);
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"age": parquet.Int(8)}),
			print: `message Test {
	required int32 age (INT(8,true));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"age": parquet.Int(16)}),
			print: `message Test {
	required int32 age (INT(16,true));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"age": parquet.Int(32)}),
			print: `message Test {
	required int32 age (INT(32,true));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"age": parquet.Int(64)}),
			print: `message Test {
	required int64 age (INT(64,true));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"age": parquet.Uint(8)}),
			print: `message Test {
	required int32 age (INT(8,false));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"age": parquet.Uint(16)}),
			print: `message Test {
	required int32 age (INT(16,false));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"age": parquet.Uint(32)}),
			print: `message Test {
	required int32 age (INT(32,false));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"age": parquet.Uint(64)}),
			print: `message Test {
	required int64 age (INT(64,false));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"ratio": parquet.Leaf(parquet.FloatType)}),
			print: `message Test {
	required float ratio;
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"ratio": parquet.Leaf(parquet.DoubleType)}),
			print: `message Test {
	required double ratio;
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"cost": parquet.Decimal(0, 9, parquet.Int32Type)}),
			print: `message Test {
	required int32 cost (DECIMAL(0,9));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"cost": parquet.Decimal(0, 18, parquet.Int64Type)}),
			print: `message Test {
	required int64 cost (DECIMAL(0,18));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"date": parquet.Date()}),
			print: `message Test {
	required int32 date (DATE);
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"time": parquet.Time(parquet.Millisecond)}),
			print: `message Test {
	required int32 time (TIME(isAdjustedToUTC=true,unit=MILLIS));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"time": parquet.Time(parquet.Microsecond)}),
			print: `message Test {
	required int64 time (TIME(isAdjustedToUTC=true,unit=MICROS));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"time": parquet.Time(parquet.Nanosecond)}),
			print: `message Test {
	required int64 time (TIME(isAdjustedToUTC=true,unit=NANOS));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"timestamp": parquet.Timestamp(parquet.Millisecond)}),
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"timestamp": parquet.Timestamp(parquet.Microsecond)}),
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=MICROS));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"timestamp": parquet.Timestamp(parquet.Nanosecond)}),
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=NANOS));
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{"names": parquet.List(parquet.String())}),
			print: `message Test {
	required group names (LIST) {
		repeated group list {
			required binary element (STRING);
		}
	}
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{
				"keys": parquet.List(
					parquet.NewGroup(parquet.GroupNodes{
						"key":   parquet.String(),
						"value": parquet.String(),
					}),
				),
			}),
			print: `message Test {
	required group keys (LIST) {
		repeated group list {
			required group element {
				required binary key (STRING);
				required binary value (STRING);
			}
		}
	}
}`,
		},

		{
			node: parquet.NewGroup(parquet.GroupNodes{
				"pairs": parquet.Map(
					parquet.String(),
					parquet.String(),
				),
			}),
			print: `message Test {
	required group pairs (MAP) {
		repeated group key_value {
			required binary key (STRING);
			required binary value (STRING);
		}
	}
}`,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			buf := new(strings.Builder)

			if err := parquet.PrintSchema(buf, "Test", test.node); err != nil {
				t.Fatal(err)
			}

			if buf.String() != test.print {
				t.Errorf("\nexpected:\n\n%s\n\nfound:\n\n%s\n", test.print, buf)
			}
		})
	}
}
