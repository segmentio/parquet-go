//go:build go1.18
// +build go1.18

package encoding_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/bytestreamsplit"
	"github.com/segmentio/parquet-go/encoding/delta"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/encoding/rle"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
	"github.com/segmentio/parquet-go/internal/fuzzing"
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
			fuzzBooleanDecoding(t, fuzzing.MakeRandBoolean(input, size), e)
			fuzzByteArrayDecoding(t, input, e)
			fuzzFixedLenByteArrayDecoding(t, size, input, e)
			fuzzFloatDecoding(t, fuzzing.MakeRandFloat(input, size), e)
			fuzzDoubleDecoding(t, fuzzing.MakeRandDouble(input, size), e)
			fuzzInt32Decoding(t, fuzzing.MakeRandInt32(input, size), e)
			fuzzInt64Decoding(t, fuzzing.MakeRandInt64(input, size), e)
			fuzzInt96Decoding(t, fuzzing.MakeRandInt96(input, size), e)
		})
	})
}

func FuzzBooleanEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, input []byte, count int) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzBooleanDecoding(t, fuzzing.MakeRandBoolean(input, count), e)
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
			fuzzFloatDecoding(t, fuzzing.MakeRandFloat(input, size), e)
			fuzzDoubleDecoding(t, fuzzing.MakeRandDouble(input, size), e)
		})
	})
}

func FuzzIntEncoding(f *testing.F) {
	f.Fuzz(func(t *testing.T, size int, input []byte) {
		fuzzEncoding(func(e encoding.Encoding) {
			fuzzInt32Decoding(t, fuzzing.MakeRandInt32(input, size), e)
			fuzzInt64Decoding(t, fuzzing.MakeRandInt64(input, size), e)
			fuzzInt96Decoding(t, fuzzing.MakeRandInt96(input, size), e)
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
