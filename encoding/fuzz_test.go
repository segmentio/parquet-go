//go:build go1.18
// +build go1.18

package encoding_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/bytestreamsplit"
	"github.com/segmentio/parquet-go/encoding/delta"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/encoding/rle"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
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

func FuzzAllEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte, size int) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzBooleanDecoding(t, makeRandBoolean(input, size), e)
			fuzzByteArrayDecoding(t, input, e)
			fuzzFixedLenByteArrayDecoding(t, size, input, e)
			fuzzFloatDecoding(t, makeRandFloat(input, size), e)
			fuzzDoubleDecoding(t, makeRandDouble(input, size), e)
			fuzzInt32Decoding(t, makeRandInt32(input, size), e)
			fuzzInt64Decoding(t, makeRandInt64(input, size), e)
			fuzzInt96Decoding(t, makeRandInt96(input, size), e)
		})
	})
}

func FuzzBooleanEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte, count int) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzBooleanDecoding(t, makeRandBoolean(input, count), e)
		})
	})
}

func FuzzByteArrayEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzByteArrayDecoding(t, input, e)
		})
	})
}

func FuzzFixedLenByteArrayEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, size int, input []byte) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzFixedLenByteArrayDecoding(t, size, input, e)
		})
	})
}

func FuzzFloatEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, size int, input []byte) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzFloatDecoding(t, makeRandFloat(input, size), e)
			fuzzDoubleDecoding(t, makeRandDouble(input, size), e)
		})
	})
}

func FuzzIntEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, size int, input []byte) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzInt32Decoding(t, makeRandInt32(input, size), e)
			fuzzInt64Decoding(t, makeRandInt64(input, size), e)
			fuzzInt96Decoding(t, makeRandInt96(input, size), e)
		})
	})
}

func fuzzBooleanDecoding(t *testing.T, input []bool, e encoding.Encoding) {
	if !e.CanEncode(format.Boolean) {
		return
	}

	buf := new(bytes.Buffer)
	dec := e.NewDecoder(buf)
	tmp := make([]bool, 1)

	for {
		_, err := dec.DecodeBoolean(tmp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Logf("encoding:%s, decoding boolean: %s", e, err)
			break
		}
	}
}

func fuzzFixedLenByteArrayDecoding(t *testing.T, size int, input []byte, e encoding.Encoding) {
	if !e.CanEncode(format.FixedLenByteArray) {
		return
	}

	dec := e.NewDecoder(bytes.NewReader(input))
	tmp := make([]byte, 1)
	for {
		_, err := dec.DecodeFixedLenByteArray(size, tmp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Logf("encoding:%s, decoding fixed len byte array: %s", e, err)
			break
		}
	}
}

func fuzzByteArrayDecoding(t *testing.T, input []byte, e encoding.Encoding) {
	if !e.CanEncode(format.ByteArray) {
		return
	}

	dec := e.NewDecoder(bytes.NewReader(input))
	tmp := encoding.MakeByteArrayList(1)
	for {
		_, err := dec.DecodeByteArray(&tmp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Logf("encoding:%s, decoding byte array: %s", e, err)
			break
		}
	}
}

func fuzzFloatDecoding(t *testing.T, input []float32, e encoding.Encoding) {
	if !e.CanEncode(format.Float) {
		return
	}

	buf := new(bytes.Buffer)
	dec := e.NewDecoder(buf)
	tmp := make([]float32, 1)
	for {
		_, err := dec.DecodeFloat(tmp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Logf("encoding:%s, decoding float: %s", e, err)
			break
		}
	}
}

func fuzzDoubleDecoding(t *testing.T, input []float64, e encoding.Encoding) {
	if !e.CanEncode(format.Double) {
		return
	}

	buf := new(bytes.Buffer)
	dec := e.NewDecoder(buf)
	tmp := make([]float64, 1)
	for {
		_, err := dec.DecodeDouble(tmp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Logf("encoding:%s, decoding double: %s", e, err)
			break
		}
	}
}

func fuzzInt32Decoding(t *testing.T, input []int32, e encoding.Encoding) {
	if !e.CanEncode(format.Int32) {
		return
	}

	buf := new(bytes.Buffer)

	dec := e.NewDecoder(buf)
	if e.String() == "RLE" {
		bitWidth := bits.MaxLen32(input)
		if bitWidth == 0 {
			bitWidth = 1
		}
		dec.SetBitWidth(bitWidth)
	}

	tmp := make([]int32, 1)
	for {
		_, err := dec.DecodeInt32(tmp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Logf("encoding:%s, decoding int32: %s", e, err)
			break
		}
	}
}

func fuzzInt64Decoding(t *testing.T, input []int64, e encoding.Encoding) {
	if !e.CanEncode(format.Int64) {
		return
	}

	buf := new(bytes.Buffer)

	dec := e.NewDecoder(buf)
	if e.String() == "RLE" {
		bitWidth := bits.MaxLen64(input)
		if bitWidth == 0 {
			bitWidth = 1
		}
		dec.SetBitWidth(bitWidth)
	}

	tmp := make([]int64, 1)
	for {
		_, err := dec.DecodeInt64(tmp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Logf("encoding:%s, decoding int64: %s", e, err)
			break
		}
	}
}

func fuzzInt96Decoding(t *testing.T, input []deprecated.Int96, e encoding.Encoding) {
	if !e.CanEncode(format.Int96) {
		return
	}

	buf := new(bytes.Buffer)
	dec := e.NewDecoder(buf)
	tmp := make([]deprecated.Int96, 1)
	for {
		_, err := dec.DecodeInt96(tmp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Logf("encoding:%s, decode: %s", e, err)
			break
		}
	}
}

func makeRandBoolean(data []byte, count int) []bool {
	if count < 1 {
		return nil
	}
	src := rand.New(newByteSource(data))
	b := make([]bool, count)
	for i := 0; i < count; i++ {
		b[i] = src.Int63()&0x01 == 1
	}
	return b
}

func makeRandFloat(data []byte, count int) []float32 {
	if count < 1 {
		return nil
	}
	src := rand.New(newByteSource(data))
	f := make([]float32, count)
	for i := 0; i < count; i++ {
		f[i] = src.Float32()
	}
	return f
}

func makeRandDouble(data []byte, count int) []float64 {
	if count < 1 {
		return nil
	}
	src := rand.New(newByteSource(data))
	f := make([]float64, count)
	for i := 0; i < count; i++ {
		f[i] = src.Float64()
	}

	return f
}

func makeRandInt32(data []byte, count int) []int32 {
	if count < 1 {
		return nil
	}

	src := rand.New(newByteSource(data))
	a := make([]int32, count)
	for i := 0; i < count; i++ {
		a[i] = int32(src.Int63())
	}
	return a
}

func makeRandInt64(data []byte, count int) []int64 {
	if count < 1 {
		return nil
	}

	src := rand.New(newByteSource(data))
	a := make([]int64, count)
	for i := 0; i < count; i++ {
		a[i] = src.Int63()
	}
	return a
}

func makeRandInt96(data []byte, count int) []deprecated.Int96 {
	if count < 1 {
		return nil
	}

	src := rand.New(newByteSource(data))
	a := make([]deprecated.Int96, count)
	for i := 0; i < count; i++ {
		a[i] = deprecated.Int96{
			uint32(src.Int63()),
			uint32(src.Int63()),
			uint32(src.Int63()),
		}
	}
	return a
}

// byteSource is used to compose fuzz tests from a byte array.
// This is to workaround the current stblib limitations.
type byteSource struct {
	*bytes.Reader
}

func newByteSource(data []byte) *byteSource {
	return &byteSource{
		Reader: bytes.NewReader(data),
	}
}

func (s *byteSource) Uint64() uint64 {
	var bytes [8]byte
	if _, err := s.Read(bytes[:]); err != nil && !errors.Is(err, io.EOF) {
		panic("byteSource: failed to read bytes")
	}
	return binary.BigEndian.Uint64(bytes[:])
}

func (s *byteSource) Int63() int64 {
	return int64(s.Uint64() >> 1)
}

func (s *byteSource) Seed(seed int64) {}
