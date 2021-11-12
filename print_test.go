package parquet_test

import (
	"strings"
	"testing"

	"github.com/segmentio/parquet"
)

func TestPrint(t *testing.T) {
	tests := []struct {
		node  parquet.Node
		print string
	}{
		{
			node:  parquet.Group{},
			print: `message Test {}`,
		},

		{
			node: parquet.Group{"name": parquet.UTF8()},
			print: `message Test {
	required binary name (UTF8);
}`,
		},

		{
			node: parquet.Group{"uuid": parquet.UUID()},
			print: `message Test {
	required fixed_len_byte_array uuid (UUID);
}`,
		},

		{
			node: parquet.Group{"enum": parquet.Enum()},
			print: `message Test {
	required binary enum (ENUM);
}`,
		},

		{
			node: parquet.Group{"json": parquet.JSON()},
			print: `message Test {
	required binary json (JSON);
}`,
		},

		{
			node: parquet.Group{"bson": parquet.BSON()},
			print: `message Test {
	required binary bson (BSON);
}`,
		},

		{
			node: parquet.Group{"name": parquet.Optional(parquet.UTF8())},
			print: `message Test {
	optional binary name (UTF8);
}`,
		},

		{
			node: parquet.Group{"name": parquet.Repeated(parquet.UTF8())},
			print: `message Test {
	repeated binary name (UTF8);
}`,
		},

		{
			node: parquet.Group{"age": parquet.Int(8)},
			print: `message Test {
	required int32 age (INT(8,true));
}`,
		},

		{
			node: parquet.Group{"age": parquet.Int(16)},
			print: `message Test {
	required int32 age (INT(16,true));
}`,
		},

		{
			node: parquet.Group{"age": parquet.Int(32)},
			print: `message Test {
	required int32 age (INT(32,true));
}`,
		},

		{
			node: parquet.Group{"age": parquet.Int(64)},
			print: `message Test {
	required int64 age (INT(64,true));
}`,
		},

		{
			node: parquet.Group{"age": parquet.Uint(8)},
			print: `message Test {
	required int32 age (INT(8,false));
}`,
		},

		{
			node: parquet.Group{"age": parquet.Uint(16)},
			print: `message Test {
	required int32 age (INT(16,false));
}`,
		},

		{
			node: parquet.Group{"age": parquet.Uint(32)},
			print: `message Test {
	required int32 age (INT(32,false));
}`,
		},

		{
			node: parquet.Group{"age": parquet.Uint(64)},
			print: `message Test {
	required int64 age (INT(64,false));
}`,
		},

		{
			node: parquet.Group{"ratio": parquet.Decimal(0, 9, parquet.FloatType)},
			print: `message Test {
	required float ratio (DECIMAL(0,9));
}`,
		},

		{
			node: parquet.Group{"ratio": parquet.Decimal(0, 18, parquet.DoubleType)},
			print: `message Test {
	required double ratio (DECIMAL(0,18));
}`,
		},

		{
			node: parquet.Group{"date": parquet.Date()},
			print: `message Test {
	required int32 date (DATE);
}`,
		},

		{
			node: parquet.Group{"time": parquet.Time(parquet.Millisecond)},
			print: `message Test {
	required int32 time (TIME(isAdjustedToUTC=true,unit=MILLIS));
}`,
		},

		{
			node: parquet.Group{"time": parquet.Time(parquet.Microsecond)},
			print: `message Test {
	required int64 time (TIME(isAdjustedToUTC=true,unit=MICROS));
}`,
		},

		{
			node: parquet.Group{"time": parquet.Time(parquet.Nanosecond)},
			print: `message Test {
	required int64 time (TIME(isAdjustedToUTC=true,unit=NANOS));
}`,
		},

		{
			node: parquet.Group{"timestamp": parquet.Timestamp(parquet.Millisecond)},
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS));
}`,
		},

		{
			node: parquet.Group{"timestamp": parquet.Timestamp(parquet.Microsecond)},
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=MICROS));
}`,
		},

		{
			node: parquet.Group{"timestamp": parquet.Timestamp(parquet.Nanosecond)},
			print: `message Test {
	required int64 timestamp (TIMESTAMP(isAdjustedToUTC=true,unit=NANOS));
}`,
		},

		{
			node: parquet.Group{"names": parquet.List(parquet.UTF8())},
			print: `message Test {
	required group names (LIST) {
		repeated group list {
			required binary element (UTF8);
		}
	}
}`,
		},

		{
			node: parquet.Group{
				"keys": parquet.List(
					parquet.Group{
						"key":   parquet.UTF8(),
						"value": parquet.UTF8(),
					},
				),
			},
			print: `message Test {
	required group keys (LIST) {
		repeated group list {
			required group element {
				required binary key (UTF8);
				required binary value (UTF8);
			}
		}
	}
}`,
		},

		{
			node: parquet.Group{
				"pairs": parquet.Map(
					parquet.UTF8(),
					parquet.UTF8(),
				),
			},
			print: `message Test {
	required group pairs (MAP) {
		repeated group key_value {
			required binary key (UTF8);
			required binary value (UTF8);
		}
	}
}`,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			buf := new(strings.Builder)

			if err := parquet.Print(buf, "Test", test.node); err != nil {
				t.Fatal(err)
			}

			if buf.String() != test.print {
				t.Errorf("\nexpected:\n\n%s\n\nfound:\n\n%s\n", test.print, buf)
			}
		})
	}
}
