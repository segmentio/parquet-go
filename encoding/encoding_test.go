package encoding_test

import (
	"bytes"
	"errors"
	"math"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet/encoding"
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

var intArrayTests = [...]encoding.IntArray{
	newIntArray(),
	newIntArray(0),
	newIntArray(1),
	newIntArray(2),
	newIntArray(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
	newIntArray(randomInt64(1, 0, math.MaxInt8, 200)...),
	newIntArray(randomInt64(2, 0, math.MaxInt16, 1000)...),
	newIntArray(randomInt64(3, 0, math.MaxInt32, 2000)...),
	newIntArray(randomInt64(4, 0, math.MaxInt64, 3000)...),
	newIntArray(randomInt64(5, math.MinInt8, math.MaxInt8, 200)...),
	newIntArray(randomInt64(6, math.MinInt16, math.MaxInt16, 1000)...),
	newIntArray(randomInt64(7, math.MinInt32, math.MaxInt32, 2000)...),
}

var fixedIntArrayTests = [...]encoding.IntArray{
	newFixedIntArray(),
	newFixedIntArray(0),
	newFixedIntArray(1),
	newFixedIntArray(2),
	newFixedIntArray(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
	newFixedIntArray(randomInt64(1, 0, math.MaxInt8, 200)...),
	newFixedIntArray(randomInt64(2, 0, math.MaxInt16, 1000)...),
	newFixedIntArray(randomInt64(3, 0, math.MaxInt32, 2000)...),
	newFixedIntArray(randomInt64(4, 0, math.MaxInt64, 3000)...),
	newFixedIntArray(randomInt64(5, math.MinInt8, math.MaxInt8, 200)...),
	newFixedIntArray(randomInt64(6, math.MinInt16, math.MaxInt16, 1000)...),
	newFixedIntArray(randomInt64(7, math.MinInt32, math.MaxInt32, 2000)...),
}

func newIntArray(values ...int64) encoding.IntArray {
	array := encoding.NewIntArray()
	appendIntArray(array, values...)
	return array
}

func newFixedIntArray(values ...int64) encoding.IntArray {
	array := encoding.NewFixedIntArray(bits.MaxLen64(values))
	appendIntArray(array, values...)
	return array
}

func appendIntArray(array encoding.IntArray, values ...int64) {
	for _, value := range values {
		array.Append(value)
	}
}

func randomInt64(seed, min, max int64, count int) []int64 {
	prng := rand.New(rand.NewSource(0))
	limit := max - min
	values := make([]int64, count)
	for i := range values {
		values[i] = prng.Int63n(limit) + min
	}
	return values
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
			scenario: "boolean",
			function: testBooleanEncoding,
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

		{
			scenario: "IntArray",
			function: testIntArrayEncoding,
		},

		{
			scenario: "FixedIntArray",
			function: testFixedIntArrayEncoding,
		},
	} {
		t.Run(test.scenario, func(t *testing.T) { test.function(t, e) })
	}
}

func testBooleanEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]bool{}
	defer enc.Close()
	defer dec.Close()

	for _, test := range booleanTests {
		t.Run("", func(t *testing.T) {
			if err := enc.EncodeBoolean(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
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

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testInt32Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]int32{}
	defer enc.Close()
	defer dec.Close()

	for _, test := range int32Tests {
		t.Run("", func(t *testing.T) {
			bitWidth := bits.MaxLen32(test)
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeInt32(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
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

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testInt64Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]int64{}
	defer enc.Close()
	defer dec.Close()

	for _, test := range int64Tests {
		t.Run("", func(t *testing.T) {
			bitWidth := bits.MaxLen64(test)
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeInt64(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
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

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testInt96Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1][12]byte{}
	defer enc.Close()
	defer dec.Close()

	for _, test := range int96Tests {
		t.Run("", func(t *testing.T) {
			bitWidth := bits.MaxLen96(test)
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeInt96(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
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

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testFloatEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]float32{}
	defer enc.Close()
	defer dec.Close()

	for _, test := range floatTests {
		t.Run("", func(t *testing.T) {
			if err := enc.EncodeFloat(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
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

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testDoubleEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1]float64{}
	defer enc.Close()
	defer dec.Close()

	for _, test := range doubleTests {
		t.Run("", func(t *testing.T) {
			if err := enc.EncodeDouble(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
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

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testByteArrayEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := [1][]byte{}
	defer enc.Close()
	defer dec.Close()

	for _, test := range byteArrayTests {
		t.Run("", func(t *testing.T) {
			if err := enc.EncodeByteArray(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
			}

			for i := range test {
				n, err := dec.DecodeByteArray(tmp[:])
				if err != nil {
					t.Fatal("decode:", err)
				}
				if n != 1 {
					t.Fatalf("decoder decoded the wrong number of items: %d", n)
				}
				if !bytes.Equal(tmp[0], test[i]) {
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[i])
				}
			}

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testFixedLenByteArrayEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	defer enc.Close()
	defer dec.Close()

	for _, test := range fixedLenByteArrayTests {
		t.Run("", func(t *testing.T) {
			tmp := make([]byte, test.size)

			if err := enc.EncodeFixedLenByteArray(test.size, test.data); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
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

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testIntArrayEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	defer enc.Close()
	defer dec.Close()

	for _, test := range intArrayTests {
		t.Run("", func(t *testing.T) {
			bitWidth := test.BitWidth()
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeIntArray(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
			}

			values := encoding.NewIntArray()
			if err := dec.DecodeIntArray(values); err != nil {
				t.Fatal("decode:", err)
			}
			if values.Len() != test.Len() {
				t.Fatalf("wrong number of values decoded into int array: want=%d got=%d", test.Len(), values.Len())
			}
			for i, n := 0, test.Len(); i < n; i++ {
				a := test.Index(i)
				b := test.Index(i)

				if a != b {
					t.Fatalf("wrong value found in decoded int array at index %d: want=%d got=%d", i, a, b)
				}
			}

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}

func testFixedIntArrayEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	defer enc.Close()
	defer dec.Close()

	for _, test := range fixedIntArrayTests {
		t.Run("", func(t *testing.T) {
			bitWidth := test.BitWidth()
			enc.SetBitWidth(bitWidth)
			dec.SetBitWidth(bitWidth)

			if err := enc.EncodeIntArray(test); err != nil {
				if errors.Is(err, encoding.ErrNotImplemented) {
					t.Skip(err)
				}
				t.Fatal("encode:", err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal("close:", err)
			}

			values := encoding.NewFixedIntArray(bitWidth)
			if err := dec.DecodeIntArray(values); err != nil {
				t.Fatal("decode:", err)
			}
			if values.Len() != test.Len() {
				t.Fatalf("wrong number of values decoded into int array: want=%d got=%d", test.Len(), values.Len())
			}
			for i, n := 0, test.Len(); i < n; i++ {
				a := test.Index(i)
				b := test.Index(i)

				if a != b {
					t.Fatalf("wrong value found in decoded int array at index %d: want=%d got=%d", i, a, b)
				}
			}

			buf.Reset()
			enc.Reset(buf)
			dec.Reset(buf)
		})
	}
}
