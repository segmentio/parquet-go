package encoding_test

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
)

var booleanTests = [...][]bool{
	{},
	{true},
	{false},
	{true, false, true, false, true, true, true, false, false, true},
}

var int32Tests = [...][]int32{
	{},
	{0},
	{1},
	{-1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxInt32, math.MaxInt32, 0},
}

var int64Tests = [...][]int64{
	{},
	{0},
	{1},
	{-1, 0, 1, 0, 2, 3, 4, 5, 6, math.MaxInt64, math.MaxInt64, 0},
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
	} {
		t.Run(test.scenario, func(t *testing.T) { test.function(t, e) })
	}
}

func testBooleanEncoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewBooleanEncoder(buf)
	dec := e.NewBooleanDecoder(buf)
	tmp := [1]bool{}

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

			for i := range test {
				n, err := dec.DecodeBoolean(tmp[:])
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

func testInt32Encoding(t *testing.T, e encoding.Encoding) {
	buf := new(bytes.Buffer)
	enc := e.NewInt32Encoder(buf)
	dec := e.NewInt32Decoder(buf)
	tmp := [1]int32{}

	for _, test := range int32Tests {
		t.Run("", func(t *testing.T) {
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
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[i])
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
	enc := e.NewInt64Encoder(buf)
	dec := e.NewInt64Decoder(buf)
	tmp := [1]int64{}

	for _, test := range int64Tests {
		t.Run("", func(t *testing.T) {
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
					t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, test[i], tmp[i])
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
	enc := e.NewInt96Encoder(buf)
	dec := e.NewInt96Decoder(buf)
	tmp := [1][12]byte{}

	for _, test := range int96Tests {
		t.Run("", func(t *testing.T) {
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
	enc := e.NewFloatEncoder(buf)
	dec := e.NewFloatDecoder(buf)
	tmp := [1]float32{}

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
	enc := e.NewDoubleEncoder(buf)
	dec := e.NewDoubleDecoder(buf)
	tmp := [1]float64{}

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
	enc := e.NewByteArrayEncoder(buf)
	dec := e.NewByteArrayDecoder(buf)
	tmp := [1][]byte{}

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
	enc := e.NewFixedLenByteArrayEncoder(buf)
	dec := e.NewFixedLenByteArrayDecoder(buf)

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
