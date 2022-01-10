//go:build go1.18
// +build go1.18

package encoding_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/bytestreamsplit"
	"github.com/segmentio/parquet/encoding/delta"
	"github.com/segmentio/parquet/encoding/plain"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
)

func fuzzEncoding(fuzz func(e encoding.Encoding)) {
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
			encoding: new(plain.DictionaryEncoding),
		},

		{
			scenario: "RLE_DICTIONARY",
			encoding: new(rle.DictionaryEncoding),
		},

		{
			scenario: "DELTA_BINARY_PACKED",
			encoding: new(delta.BinaryPackedEncoding),
		},

		{
			scenario: "DELTA_LENGTH_BYTE_ARRAY",
			encoding: new(delta.LengthByteArrayEncoding),
		},

		{
			scenario: "DELTA_BYTE_ARRAY",
			encoding: new(delta.ByteArrayEncoding),
		},

		{
			scenario: "BYTE_STREAM_SPLIT",
			encoding: new(bytestreamsplit.Encoding),
		},
	} {
		fuzz(test.encoding)
	}
}

func FuzzByteArrayEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzByteArrayEncoding(t, input, e)
		})
	})
}

func FuzzFixedLenByteArrayEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, size int, input []byte) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzFixedLenByteArrayEncoding(t, size, input, e)
		})
	})
}

func fuzzFixedLenByteArrayEncoding(t *testing.T, size int, input []byte, e encoding.Encoding) {
	if size < 0 || size >= 1<<60 {
		return
	}

	if !e.CanEncode(format.FixedLenByteArray) {
		return
	}

	t.Logf("encoding: %s", e.String())
	t.Logf("fuzzing data - size=%d input=%v", size, input)

	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)

	defer dec.Reset(buf)
	defer enc.Reset(buf)
	defer buf.Reset()

	tmp := make([]byte, size)

	if err := enc.EncodeFixedLenByteArray(size, input); err != nil {
		if errors.Is(err, encoding.ErrNotSupported) {
			return
		}

		if errors.Is(err, encoding.ErrInvalidArguments) {
			return
		}
		t.Fatal("encode: ", err)
	}

	for i := 0; i < (len(input) / size); i++ {
		n, err := dec.DecodeFixedLenByteArray(size, tmp)
		if err != nil && (err != io.EOF || n != len(input)) {
			t.Fatal("decode: ", err)
		}

		want := input[i*size : (i+1)*size]
		if !bytes.Equal(want, tmp) {
			t.Fatalf("decoder decoded the wrong value at index %d:\nwant = %#v\ngot  = %#v", i, want, tmp)
		}
	}

	if n, err := dec.DecodeFixedLenByteArray(size, tmp); err != nil && err != io.EOF {
		t.Fatal("non-EOF error returned after decoding all the values:", err)
	} else if n != 0 {
		t.Fatal("non-zero number of values decoded at EOF:", n)
	}
}

func fuzzByteArrayEncoding(t *testing.T, input []byte, e encoding.Encoding) {
	if !e.CanEncode(format.ByteArray) {
		return
	}

	buf := new(bytes.Buffer)
	enc := e.NewEncoder(buf)
	dec := e.NewDecoder(buf)
	tmp := encoding.MakeByteArrayList(1)

	defer dec.Reset(buf)
	defer enc.Reset(buf)
	defer buf.Reset()
	defer tmp.Reset()

	tmp.Push(input)

	if err := enc.EncodeByteArray(tmp); err != nil {
		if errors.Is(err, encoding.ErrNotSupported) {
			t.Skip(err)
		}

		t.Fatal("encode: ", err)
	}

	tmp.Reset()

	n, err := dec.DecodeByteArray(&tmp)
	if err != nil && (err != io.EOF || n != len(input)) {
		t.Fatal("decode:", n, err)
	}

	tmp.Range(func(value []byte) bool {
		if bytes.Compare(value, input) != 0 {
			t.Fatalf("%v", value)
		}
		return true
	})

	tmp.Reset()

	if n, err := dec.DecodeByteArray(&tmp); err != io.EOF {
		t.Error("non-EOF error returned after decoding all the values:", err)
	} else if n != 0 {
		t.Error("non-zero number of values decoded at EOF:", n)
	}
}
