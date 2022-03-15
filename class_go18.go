//go:build go1.18

package parquet

import (
	"unsafe"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/encoding/plain"
	"github.com/segmentio/parquet-go/internal/bits"
	"github.com/segmentio/parquet-go/internal/cast"
)

type primitive interface {
	bool | int32 | int64 | deprecated.Int96 | float32 | float64 | uint32 | uint64
}

func sizeof[T primitive]() int {
	var z T
	return int(unsafe.Sizeof(z))
}

type comparable interface {
	int32 | int64 | float32 | float64 | uint32 | uint64
}

func compare[T comparable](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return +1
	default:
		return 0
	}
}

func less[T comparable](a, b T) bool {
	return a < b
}

type class[T primitive] struct {
	name      string
	makeValue func(T) Value
	valueOf   func(Value) T
	plain     func(T) []byte
	compare   func(T, T) int
	less      func(T, T) bool
	min       func([]T) T
	max       func([]T) T
	bounds    func([]T) (T, T)
	encode    func(encoding.Encoder, []T) error
	decode    func(encoding.Decoder, []T) (int, error)
}

var boolClass = class[bool]{
	name:      "BOOLEAN",
	makeValue: makeValueBoolean,
	valueOf:   Value.Boolean,
	plain:     plain.Boolean,
	compare:   compareBool,
	less:      func(a, b bool) bool { return a != b && !a },
	min:       bits.MinBool,
	max:       bits.MaxBool,
	bounds:    bits.MinMaxBool,
	encode:    encoding.Encoder.EncodeBoolean,
	decode:    encoding.Decoder.DecodeBoolean,
}

var int32Class = class[int32]{
	makeValue: makeValueInt32,
	valueOf:   Value.Int32,
	plain:     plain.Int32,
	compare:   compare[int32],
	less:      less[int32],
	min:       bits.MinInt32,
	max:       bits.MaxInt32,
	bounds:    bits.MinMaxInt32,
	encode:    encoding.Encoder.EncodeInt32,
	decode:    encoding.Decoder.DecodeInt32,
}

var int64Class = class[int64]{
	makeValue: makeValueInt64,
	valueOf:   Value.Int64,
	plain:     plain.Int64,
	compare:   compare[int64],
	less:      less[int64],
	min:       bits.MinInt64,
	max:       bits.MaxInt64,
	bounds:    bits.MinMaxInt64,
	encode:    encoding.Encoder.EncodeInt64,
	decode:    encoding.Decoder.DecodeInt64,
}

var int96Class = class[deprecated.Int96]{
	makeValue: makeValueInt96,
	valueOf:   Value.Int96,
	plain:     plain.Int96,
	compare:   compareInt96,
	less:      deprecated.Int96.Less,
	min:       deprecated.MinInt96,
	max:       deprecated.MaxInt96,
	bounds:    deprecated.MinMaxInt96,
	encode:    encoding.Encoder.EncodeInt96,
	decode:    encoding.Decoder.DecodeInt96,
}

var float32Class = class[float32]{
	makeValue: makeValueFloat,
	valueOf:   Value.Float,
	plain:     plain.Float,
	compare:   compare[float32],
	less:      less[float32],
	min:       bits.MinFloat32,
	max:       bits.MaxFloat32,
	bounds:    bits.MinMaxFloat32,
	encode:    encoding.Encoder.EncodeFloat,
	decode:    encoding.Decoder.DecodeFloat,
}

var float64Class = class[float64]{
	makeValue: makeValueDouble,
	valueOf:   Value.Double,
	plain:     plain.Double,
	compare:   compare[float64],
	less:      less[float64],
	min:       bits.MinFloat64,
	max:       bits.MaxFloat64,
	bounds:    bits.MinMaxFloat64,
	encode:    encoding.Encoder.EncodeDouble,
	decode:    encoding.Decoder.DecodeDouble,
}

var uint32Class = class[uint32]{
	makeValue: func(v uint32) Value { return makeValueInt32(int32(v)) },
	valueOf:   func(v Value) uint32 { return uint32(v.Int32()) },
	plain:     func(v uint32) []byte { return plain.Int32(int32(v)) },
	compare:   compare[uint32],
	less:      less[uint32],
	min:       bits.MinUint32,
	max:       bits.MaxUint32,
	bounds:    bits.MinMaxUint32,
	encode: func(e encoding.Encoder, v []uint32) error {
		return e.EncodeInt32(cast.Slice[int32](v))
	},
	decode: func(d encoding.Decoder, v []uint32) (int, error) {
		return d.DecodeInt32(cast.Slice[int32](v))
	},
}

var uint64Class = class[uint64]{
	makeValue: func(v uint64) Value { return makeValueInt64(int64(v)) },
	valueOf:   func(v Value) uint64 { return uint64(v.Int64()) },
	plain:     func(v uint64) []byte { return plain.Int64(int64(v)) },
	compare:   compare[uint64],
	less:      less[uint64],
	min:       bits.MinUint64,
	max:       bits.MaxUint64,
	bounds:    bits.MinMaxUint64,
	encode: func(e encoding.Encoder, v []uint64) error {
		return e.EncodeInt64(cast.Slice[int64](v))
	},
	decode: func(d encoding.Decoder, v []uint64) (int, error) {
		return d.DecodeInt64(cast.Slice[int64](v))
	},
}
