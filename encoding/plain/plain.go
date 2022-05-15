// Package plain implements the PLAIN parquet encoding.
//
// https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0
package plain

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

const (
	ByteArrayLengthSize = 4
)

type Encoding struct {
	encoding.NotSupported
}

func (e *Encoding) String() string {
	return "PLAIN"
}

func (e *Encoding) Encoding() format.Encoding {
	return format.Plain
}

func (e *Encoding) EncodeBoolean(dst, src []byte) ([]byte, error) {
	dst = dst[:0]
	b := byte(0)
	i := 0
	n := (len(src) / 8) * 8

	for i < n {
		b = 0
		if src[i+7] != 0 {
			b |= 1 << 7
		}
		if src[i+6] != 0 {
			b |= 1 << 6
		}
		if src[i+5] != 0 {
			b |= 1 << 5
		}
		if src[i+4] != 0 {
			b |= 1 << 4
		}
		if src[i+3] != 0 {
			b |= 1 << 3
		}
		if src[i+2] != 0 {
			b |= 1 << 2
		}
		if src[i+1] != 0 {
			b |= 1 << 1
		}
		if src[i+0] != 0 {
			b |= 1 << 0
		}
		dst = append(dst, b)
		i += 8
	}

	if i < len(src) {
		b = 0
		for j := uint(0); i < len(src); j++ {
			if src[i] != 0 {
				b |= 1 << j
			}
			i++
		}
		dst = append(dst, b)
	}

	return dst, nil
}

func (e *Encoding) EncodeInt32(dst, src []byte) ([]byte, error) {
	if (len(src) % 4) != 0 {
		return dst[:0], encoding.ErrEncodeInvalidInputSize(e, "INT32", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) EncodeInt64(dst, src []byte) ([]byte, error) {
	if (len(src) % 8) != 0 {
		return dst[:0], encoding.ErrEncodeInvalidInputSize(e, "INT64", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) EncodeInt96(dst, src []byte) ([]byte, error) {
	if (len(src) % 12) != 0 {
		return dst[:0], encoding.ErrEncodeInvalidInputSize(e, "INT96", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) EncodeFloat(dst, src []byte) ([]byte, error) {
	if (len(src) % 4) != 0 {
		return dst[:0], encoding.ErrEncodeInvalidInputSize(e, "FLOAT", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) EncodeDouble(dst, src []byte) ([]byte, error) {
	if (len(src) % 8) != 0 {
		return dst[:0], encoding.ErrEncodeInvalidInputSize(e, "DOUBLE", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) EncodeByteArray(dst []byte, src []byte) ([]byte, error) {
	if err := RangeByteArrays(src, func([]byte) error { return nil }); err != nil {
		return dst[:0], encoding.Error(e, err)
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) EncodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error) {
	if size < 0 || size > encoding.MaxFixedLenByteArraySize {
		return dst[:0], encoding.Error(e, encoding.ErrInvalidArgument)
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) DecodeBoolean(dst, src []byte) ([]byte, error) {
	dst = dst[:0]
	for _, b := range src {
		dst = append(dst,
			((b >> 0) & 1),
			((b >> 1) & 1),
			((b >> 2) & 1),
			((b >> 3) & 1),
			((b >> 4) & 1),
			((b >> 5) & 1),
			((b >> 6) & 1),
			((b >> 7) & 1),
		)
	}
	return dst, nil
}

func (e *Encoding) DecodeInt32(dst, src []byte) ([]byte, error) {
	if (len(src) % 4) != 0 {
		return dst[:0], encoding.ErrDecodeInvalidInputSize(e, "INT32", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) DecodeInt64(dst, src []byte) ([]byte, error) {
	if (len(src) % 8) != 0 {
		return dst[:0], encoding.ErrDecodeInvalidInputSize(e, "INT64", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) DecodeInt96(dst, src []byte) ([]byte, error) {
	if (len(src) % 12) != 0 {
		return dst[:0], encoding.ErrDecodeInvalidInputSize(e, "INT96", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) DecodeFloat(dst, src []byte) ([]byte, error) {
	if (len(src) % 4) != 0 {
		return dst[:0], encoding.ErrDecodeInvalidInputSize(e, "FLOAT", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) DecodeDouble(dst, src []byte) ([]byte, error) {
	if (len(src) % 8) != 0 {
		return dst[:0], encoding.ErrDecodeInvalidInputSize(e, "DOUBLE", len(src))
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) DecodeByteArray(dst, src []byte) ([]byte, error) {
	if err := RangeByteArrays(src, func([]byte) error { return nil }); err != nil {
		return dst[:0], encoding.Error(e, err)
	}
	return append(dst[:0], src...), nil
}

func (e *Encoding) DecodeFixedLenByteArray(dst, src []byte, size int) ([]byte, error) {
	if size < 0 || size > encoding.MaxFixedLenByteArraySize {
		return dst[:0], encoding.Error(e, encoding.ErrInvalidArgument)
	}
	if (len(src) % size) != 0 {
		return dst[:0], encoding.ErrDecodeInvalidInputSize(e, "FIXED_LEN_BYTE_ARRAY", len(src))
	}
	return append(dst[:0], src...), nil
}

func Boolean(v bool) []byte { return AppendBoolean(nil, v) }

func Int32(v int32) []byte { return AppendInt32(nil, v) }

func Int64(v int64) []byte { return AppendInt64(nil, v) }

func Int96(v deprecated.Int96) []byte { return AppendInt96(nil, v) }

func Float(v float32) []byte { return AppendFloat(nil, v) }

func Double(v float64) []byte { return AppendDouble(nil, v) }

func ByteArray(v []byte) []byte { return AppendByteArray(nil, v) }

func AppendBoolean(b []byte, v bool) []byte {
	if v {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return b
}

func AppendInt32(b []byte, v int32) []byte {
	x := [4]byte{}
	binary.LittleEndian.PutUint32(x[:], uint32(v))
	return append(b, x[:]...)
}

func AppendInt64(b []byte, v int64) []byte {
	x := [8]byte{}
	binary.LittleEndian.PutUint64(x[:], uint64(v))
	return append(b, x[:]...)
}

func AppendInt96(b []byte, v deprecated.Int96) []byte {
	x := [12]byte{}
	binary.LittleEndian.PutUint32(x[0:4], v[0])
	binary.LittleEndian.PutUint32(x[4:8], v[1])
	binary.LittleEndian.PutUint32(x[8:12], v[2])
	return append(b, x[:]...)
}

func AppendFloat(b []byte, v float32) []byte {
	x := [4]byte{}
	binary.LittleEndian.PutUint32(x[:], math.Float32bits(v))
	return append(b, x[:]...)
}

func AppendDouble(b []byte, v float64) []byte {
	x := [8]byte{}
	binary.LittleEndian.PutUint64(x[:], math.Float64bits(v))
	return append(b, x[:]...)
}

func AppendByteArray(b, v []byte) []byte {
	i := len(b)
	j := i + 4
	b = append(b, 0, 0, 0, 0)
	b = append(b, v...)
	PutByteArrayLength(b[i:j:j], len(v))
	return b
}

func ByteArrayLength(b []byte) int {
	return int(binary.LittleEndian.Uint32(b))
}

func PutByteArrayLength(b []byte, n int) {
	binary.LittleEndian.PutUint32(b, uint32(n))
}

func RangeByteArrays(b []byte, do func([]byte) error) (err error) {
	for len(b) > 0 {
		var v []byte
		if v, b, err = NextByteArray(b); err != nil {
			return err
		}
		if err = do(v); err != nil {
			return err
		}
	}
	return nil
}

func NextByteArray(b []byte) (v, r []byte, err error) {
	if len(b) < 4 {
		return nil, b, fmt.Errorf("input of length %d is too short to contain a PLAIN encoded byte array: %w", len(b), io.ErrUnexpectedEOF)
	}
	n := 4 + int(binary.LittleEndian.Uint32(b))
	if n > len(b) {
		return nil, b, fmt.Errorf("input of length %d is too short to contain a PLAIN encoded byte array of length %d: %w", len(b)-4, n-4, io.ErrUnexpectedEOF)
	}
	return b[4:n:n], b[n:len(b):len(b)], nil
}
