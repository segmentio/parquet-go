package deprecated_test

import (
	"fmt"
	"testing"

	"github.com/segmentio/parquet-go/deprecated"
)

func TestInt96Less(t *testing.T) {
	tests := []struct {
		i    deprecated.Int96
		j    deprecated.Int96
		less bool
	}{
		{
			i:    deprecated.Int96{},
			j:    deprecated.Int96{},
			less: false,
		},

		{
			i:    deprecated.Int96{0: 1},
			j:    deprecated.Int96{0: 2},
			less: true,
		},

		{
			i:    deprecated.Int96{0: 1},
			j:    deprecated.Int96{1: 1},
			less: true,
		},

		{
			i:    deprecated.Int96{0: 1},
			j:    deprecated.Int96{2: 1},
			less: true,
		},

		{
			i:    deprecated.Int96{0: 0xFFFFFFFF, 1: 0xFFFFFFFF, 2: 0xFFFFFFFF}, // -1
			j:    deprecated.Int96{},                                            // 0
			less: true,
		},

		{
			i:    deprecated.Int96{},                                            // 0
			j:    deprecated.Int96{0: 0xFFFFFFFF, 1: 0xFFFFFFFF, 2: 0xFFFFFFFF}, // -1
			less: false,
		},

		{
			i:    deprecated.Int96{0: 0xFFFFFFFF, 1: 0xFFFFFFFF, 2: 0xFFFFFFFF}, // -1
			j:    deprecated.Int96{0: 0xFFFFFFFF, 1: 0xFFFFFFFF, 2: 0xFFFFFFFF}, // -1
			less: false,
		},

		{
			i:    deprecated.Int96{0: 0xFFFFFFFF, 1: 0xFFFFFFFF, 2: 0xFFFFFFFF}, // -1
			j:    deprecated.Int96{0: 0xFFFFFFFE, 1: 0xFFFFFFFF, 2: 0xFFFFFFFF}, // -2
			less: false,
		},

		{
			i:    deprecated.Int96{0: 0xFFFFFFFE, 1: 0xFFFFFFFF, 2: 0xFFFFFFFF}, // -2
			j:    deprecated.Int96{0: 0xFFFFFFFF, 1: 0xFFFFFFFF, 2: 0xFFFFFFFF}, // -1
			less: true,
		},
	}

	for _, test := range tests {
		scenario := ""
		if test.less {
			scenario = fmt.Sprintf("%s<%s", test.i, test.j)
		} else {
			scenario = fmt.Sprintf("%s>=%s", test.i, test.j)
		}
		t.Run(scenario, func(t *testing.T) {
			if test.i.Less(test.j) != test.less {
				t.Error("FAIL")
			}
			if test.less {
				if test.j.Less(test.i) {
					t.Error("FAIL (inverse)")
				}
			}
		})
	}
}

func TestMaxLenInt96(t *testing.T) {
	for _, test := range []struct {
		data   []deprecated.Int96
		maxlen int
	}{
		{
			data:   nil,
			maxlen: 0,
		},

		{
			data:   []deprecated.Int96{{}, {}, {}, {}, {}},
			maxlen: 0,
		},

		{
			data:   []deprecated.Int96{{0: 0x01}, {0: 0xFF}, {1: 0x02}, {0: 0xF0}},
			maxlen: 34,
		},
	} {
		t.Run("", func(t *testing.T) {
			if maxlen := deprecated.MaxLenInt96(test.data); maxlen != test.maxlen {
				t.Errorf("want=%d got=%d", test.maxlen, maxlen)
			}
		})
	}
}
