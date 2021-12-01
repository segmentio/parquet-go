package encoding_test

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/bytestreamsplit"
	"github.com/segmentio/parquet/encoding/dict"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/internal/bits"
)

var booleanTests = [...][]bool{
	{},
	{true},
	{false},
	{true, false, true, false, true, true, true, false, false, true},
	{ // repeating 32x
		true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true,
	},
	{ // repeating 33x
		true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true,
		true,
	},
	{ // alternating 15x
		false, true, false, true, false, true, false, true,
		false, true, false, true, false, true, false,
	},
	{ // alternating 16x
		false, true, false, true, false, true, false, true,
		false, true, false, true, false, true, false, true,
	},
}

var int8Tests = [...][]int8{
	{},
	{0},
	{1},
	{-1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxInt8, math.MaxInt8, 0},
	{ // repeating 24x
		42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42,
	},
	{ // never repeating
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
	},
	{ // strakes of repeating values
		0, 0, 0, 0, 1, 1, 1, 1,
		2, 2, 2, 2, 3, 3, 3, 3,
		4, 4, 4, 4, 5, 5, 5, 5,
		6, 6, 6, 7, 7, 7, 8, 8,
		8, 9, 9, 9,
	},
}

var int16Tests = [...][]int16{
	{},
	{0},
	{1},
	{-1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxInt16, math.MaxInt16, 0},
	{ // repeating 24x
		42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42,
	},
	{ // never repeating
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
	},
	{ // strakes of repeating values
		0, 0, 0, 0, 1, 1, 1, 1,
		2, 2, 2, 2, 3, 3, 3, 3,
		4, 4, 4, 4, 5, 5, 5, 5,
		6, 6, 6, 7, 7, 7, 8, 8,
		8, 9, 9, 9,
	},
}

var int32Tests = [...][]int32{
	{},
	{0},
	{1},
	{-1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxInt32, math.MaxInt32, 0},
	{ // repeating 24x
		42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42,
	},
	{ // never repeating
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
	},
	{ // strakes of repeating values
		0, 0, 0, 0, 1, 1, 1, 1,
		2, 2, 2, 2, 3, 3, 3, 3,
		4, 4, 4, 4, 5, 5, 5, 5,
		6, 6, 6, 7, 7, 7, 8, 8,
		8, 9, 9, 9,
	},
}

var int64Tests = [...][]int64{
	{},
	{0},
	{1},
	{-1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxInt64, math.MaxInt64, 0},
	{ // repeating 24x
		42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42,
	},
	{ // never repeating
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
	},
	{ // strakes of repeating values
		0, 0, 0, 0, 1, 1, 1, 1,
		2, 2, 2, 2, 3, 3, 3, 3,
		4, 4, 4, 4, 5, 5, 5, 5,
		6, 6, 6, 7, 7, 7, 8, 8,
		8, 9, 9, 9,
	},
	{ // strakes of repeating values
		0, 0, 0, 0, 1, 1, 1, 1,
		2, 2, 2, 2, 3, 3, 3, 3,
		4, 4, 4, 4, 5, 5, 5, 5,
		6, 6, 6, 7, 7, 7, 8, 8,
		8, 9, 9, 9,
	},
}

var int96Tests = [...][][12]byte{
	{},
	{{0: 0}},
	{{0: 1}},
}

var floatTests = [...][]float32{
	{},
	{0},
	{1},
	{0, 1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxFloat32, math.MaxFloat32, 0},
	{-1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxFloat32, math.MaxFloat32, 0},
}

var doubleTests = [...][]float64{
	{},
	{0},
	{1},
	{-1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxFloat64, math.MaxFloat64, 0},
}

var byteArrayTests = [...][][]byte{
	{},
	{[]byte("")},
	{[]byte("A"), []byte("B"), []byte("C")},
	{[]byte("hello world!"), bytes.Repeat([]byte("1234567890"), 100)},
}

var fixedLenByteArrayTests = [...]struct {
	size int
	data []byte
}{
	{size: 1, data: []byte("")},
	{size: 1, data: []byte("ABCDEFGH")},
	{size: 2, data: []byte("ABCDEFGH")},
	{size: 4, data: []byte("ABCDEFGH")},
	{size: 8, data: []byte("ABCDEFGH")},
}

func TestEncoding(t *testing.T) {
	for _, test := range [...]struct {
		scenario string
		encoding encoding.Encoding
	}{
		{
			scenario: "PLAIN",
			encoding: new(plain.Encoding),
		},

		{
			scenario: "RLE",
			encoding: new(rle.Encoding),
		},

		{
			scenario: "PLAIN_DICTIONARY",
			encoding: new(dict.Encoding).PlainEncoding(),
		},

		{
			scenario: "RLE_DICTIONARY",
			encoding: new(dict.Encoding),
		},

		{
			scenario: "BYTE_STREAM_SPLIT",
			encoding: new(bytestreamsplit.Encoding),
		},
	} {
		t.Run(test.scenario, func(t *testing.T) { testEncoding(t, test.encoding) })
	}
}

func testEncoding(t *testing.T, e encoding.Encoding) {
	for _, test := range [...]struct {
		scenario string
		function func(*testing.T, encoding.Encoding)
	}{
		{
			scenario: "encoding",
			function: testFormatEncoding,
		},

		{
			scenario: "boolean",
			function: testBooleanEncoding,
		},

		{
			scenario: "int8",
			function: testInt8Encoding,
		},

		{
			scenario: "int16",
			function: testInt16Encoding,
		},

		{
			scenario: "int32",
			function: testInt32Encoding,
		},

		{
			scenario: "int64",
			function: testInt64Encoding,
		},

		{
			scenario: "int96",
			function: testInt96Encoding,
		},

		{
			scenario: "float",
			function: testFloatEncoding,
		},

		{
			scenario: "double",
			function: testDoubleEncoding,
		},

		{
			scenario: "byte array",
			function: testByteArrayEncoding,
		},

		{
			scenario: "fixed length byte array",
			function: testFixedLenByteArrayEncoding,
		},
	} {
		t.Run(test.scenario, func(t *testing.T) { test.function(t, e) })
	}
}

func testFormatEncoding(t *testing.T, e encoding.Encoding) {
	enc := e.NewEncoder(nil)
	dec := e.NewDecoder(nil)

	if enc.Encoding() != e.Encoding() {
		t.Errorf("wrong encoder encoding: want=%s got=%s", e.Encoding(), enc.Encoding())
	}

	if dec.Encoding() != e.Encoding() {
		t.Errorf("wrong decoder encoding: want=%s got=%s", e.Encoding(), dec.Encoding())
	}
}

func testBooleanEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]bool{}

	for _, test := range booleanTests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()

			if err := enc.EncodeBoolean(test); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i, want := range test {
				n, err := dec.DecodeBoolean(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if got := tmp[0]; got != want {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, want, got)
				}
			}

			if n, err := dec.DecodeBoolean(tmp[:]); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testInt8Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]int8{}

	for _, test := range int8Tests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()

			bitWidth := bits.MaxLen8(test)
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeInt8(test); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i := range test {
				n, err := dec.DecodeInt8(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if tmp[0] != test[i] {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[0])
				}
			}

			if n, err := dec.DecodeInt8(tmp[:]); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testInt16Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]int16{}

	for _, test := range int16Tests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()

			bitWidth := bits.MaxLen16(test)
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeInt16(test); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i := range test {
				n, err := dec.DecodeInt16(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if tmp[0] != test[i] {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[0])
				}
			}

			if n, err := dec.DecodeInt16(tmp[:]); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testInt32Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]int32{}

	for _, test := range int32Tests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()

			bitWidth := bits.MaxLen32(test)
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeInt32(test); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i := range test {
				n, err := dec.DecodeInt32(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if tmp[0] != test[i] {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[0])
				}
			}

			if n, err := dec.DecodeInt32(tmp[:]); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testInt64Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]int64{}

	for _, test := range int64Tests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()

			bitWidth := bits.MaxLen64(test)
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeInt64(test); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i := range test {
				n, err := dec.DecodeInt64(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if tmp[0] != test[i] {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[0])
				}
			}

			if n, err := dec.DecodeInt64(tmp[:]); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testInt96Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1][12]byte{}

	for _, test := range int96Tests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()

			bitWidth := bits.MaxLen96(test)
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeInt96(test); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i := range test {
				n, err := dec.DecodeInt96(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if tmp[0] != test[i] {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[i])
				}
			}

			if n, err := dec.DecodeInt96(tmp[:]); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testFloatEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]float32{}

	for _, test := range floatTests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()

			if err := enc.EncodeFloat(test); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i := range test {
				n, err := dec.DecodeFloat(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if tmp[0] != test[i] {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[i])
				}
			}

			if n, err := dec.DecodeFloat(tmp[:]); err != nil && err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testDoubleEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]float64{}

	for _, test := range doubleTests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()

			if err := enc.EncodeDouble(test); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i := range test {
				n, err := dec.DecodeDouble(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if tmp[0] != test[i] {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[i])
				}
			}

			if n, err := dec.DecodeDouble(tmp[:]); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testByteArrayEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := make([]byte, 0, 4096)

	for _, test := range byteArrayTests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()
			tmp = plain.AppendByteArrayList(tmp[:0], test...)

			if err := enc.EncodeByteArray(tmp); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			maxLen := 0
			for _, value := range test {
				if len(value) > maxLen {
					maxLen = len(value)
				}
			}

			for i := 0; i < len(test); {
				for i := range tmp {
					tmp[i] = 0
				}
				n, err := dec.DecodeByteArray(tmp[:4+maxLen])
				if err != nil && (err != io.EOF || n != len(test)) {
					t.Fatal("decode:", err)
				}
				if n == 0 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				plain.ScanByteArrayList(tmp, n, func(value []byte) error {
					if !bytes.Equal(value, test[i]) {
						t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], value)
					}
					i++
					return nil
				})
			}

			if n, err := dec.DecodeByteArray(tmp[:4+maxLen]); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}

func testFixedLenByteArrayEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)

	for _, test := range fixedLenByteArrayTests {
		t.Run("", func(t *testing.T) {
			defer dec.Reset(buf)
			defer enc.Reset(buf)
			defer buf.Reset()
			tmp := make([]byte, test.size)

			if err := enc.EncodeFixedLenByteArray(test.size, test.data); err != nil {
				if errors.Is(err, encoding.ErrNotSupported) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}

			for i := 0; i < (len(test.data) / test.size); i++ {
				n, err := dec.DecodeFixedLenByteArray(test.size, tmp)
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				want := test.data[i*test.size : (i+1)*test.size]
				if !bytes.Equal(want, tmp) {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, want, tmp)
				}
			}

			if n, err := dec.DecodeFixedLenByteArray(test.size, tmp); err != io.EOF {
				t.Fatal("non-EOF error returned after decoding all the values:", err)
			} else if n != 0 {
				t.Fatal("non-zero number of values decoded at EOF:", n)
			}
		})
	}
}
