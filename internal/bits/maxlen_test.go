package bits_test

import (
	"testing"

	"github.com/segmentio/parquet/internal/bits"
)

func TestMaxLen8(t *testing.T) {
	for _, test := range []struct {
		data   []int8
		maxlen int
	}{
		{
			data:   nil,
			maxlen: 0,
		},

		{
			data:   []int8{0, 0, 0, 0, 0},
			maxlen: 0,
		},

		{
			data:   []int8{10, 0, -1, 0},
			maxlen: 8,
		},
	} {
		t.Run("", func(t *testing.T) {
			if maxlen := bits.MaxLen8(test.data); maxlen != test.maxlen {
				t.Errorf("want=%d got=%d", test.maxlen, maxlen)
			}
		})
	}
}

func TestMaxLen16(t *testing.T) {
	for _, test := range []struct {
		data   []int16
		maxlen int
	}{
		{
			data:   nil,
			maxlen: 0,
		},

		{
			data:   []int16{0, 0, 0, 0, 0},
			maxlen: 0,
		},

		{
			data:   []int16{10, 0, -1, 0},
			maxlen: 16,
		},
	} {
		t.Run("", func(t *testing.T) {
			if maxlen := bits.MaxLen16(test.data); maxlen != test.maxlen {
				t.Errorf("want=%d got=%d", test.maxlen, maxlen)
			}
		})
	}
}

func TestMaxLen32(t *testing.T) {
	for _, test := range []struct {
		data   []int32
		maxlen int
	}{
		{
			data:   nil,
			maxlen: 0,
		},

		{
			data:   []int32{0, 0, 0, 0, 0},
			maxlen: 0,
		},

		{
			data:   []int32{0x00000010, 0x0000F000, 0x00000990, 0x00000000},
			maxlen: 16,
		},
	} {
		t.Run("", func(t *testing.T) {
			if maxlen := bits.MaxLen32(test.data); maxlen != test.maxlen {
				t.Errorf("want=%d got=%d", test.maxlen, maxlen)
			}
		})
	}
}

func TestMaxLen64(t *testing.T) {
	for _, test := range []struct {
		data   []int64
		maxlen int
	}{
		{
			data:   nil,
			maxlen: 0,
		},

		{
			data:   []int64{0, 0, 0, 0, 0},
			maxlen: 0,
		},

		{
			data:   []int64{0x00000010, 0x0000F000, 0x00000990, 0x00000000},
			maxlen: 16,
		},
	} {
		t.Run("", func(t *testing.T) {
			if maxlen := bits.MaxLen64(test.data); maxlen != test.maxlen {
				t.Errorf("want=%d got=%d", test.maxlen, maxlen)
			}
		})
	}
}
