//go:build go1.18

package parquet

import (
	"unsafe"

	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/internal/bits"
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
	bits      int8
	kind      Kind
	makeValue func(T) Value
	value     func(Value) T
	compare   func(T, T) int
	less      func(T, T) bool
	order     func([]T) int
	min       func([]T) T
	max       func([]T) T
	bounds    func([]T) (T, T)
	encode2   func(encoding.Encoding, []byte, []byte) ([]byte, error)
	decode2   func(encoding.Encoding, []byte, []byte) ([]byte, error)
}

var boolClass = class[bool]{
	name:      "BOOLEAN",
	bits:      1,
	kind:      Boolean,
	makeValue: makeValueBoolean,
	value:     Value.Boolean,
	compare:   compareBool,
	less:      func(a, b bool) bool { return a != b && !a },
	order:     bits.OrderOfBool,
	min:       bits.MinBool,
	max:       bits.MaxBool,
	bounds:    bits.MinMaxBool,
	encode2:   encoding.Encoding.EncodeBoolean,
	decode2:   encoding.Encoding.DecodeBoolean,
}

var int32Class = class[int32]{
	name:      "INT32",
	bits:      32,
	kind:      Int32,
	makeValue: makeValueInt32,
	value:     Value.Int32,
	compare:   compare[int32],
	less:      less[int32],
	order:     bits.OrderOfInt32,
	min:       bits.MinInt32,
	max:       bits.MaxInt32,
	bounds:    bits.MinMaxInt32,
	encode2:   encoding.Encoding.EncodeInt32,
	decode2:   encoding.Encoding.DecodeInt32,
}

var int64Class = class[int64]{
	name:      "INT64",
	bits:      64,
	kind:      Int64,
	makeValue: makeValueInt64,
	value:     Value.Int64,
	compare:   compare[int64],
	less:      less[int64],
	order:     bits.OrderOfInt64,
	min:       bits.MinInt64,
	max:       bits.MaxInt64,
	bounds:    bits.MinMaxInt64,
	encode2:   encoding.Encoding.EncodeInt64,
	decode2:   encoding.Encoding.DecodeInt64,
}

var int96Class = class[deprecated.Int96]{
	name:      "INT96",
	bits:      96,
	kind:      Int96,
	makeValue: makeValueInt96,
	value:     Value.Int96,
	compare:   compareInt96,
	less:      deprecated.Int96.Less,
	order:     deprecated.OrderOfInt96,
	min:       deprecated.MinInt96,
	max:       deprecated.MaxInt96,
	bounds:    deprecated.MinMaxInt96,
	encode2:   encoding.Encoding.EncodeInt96,
	decode2:   encoding.Encoding.DecodeInt96,
}

var float32Class = class[float32]{
	name:      "FLOAT",
	bits:      32,
	kind:      Float,
	makeValue: makeValueFloat,
	value:     Value.Float,
	compare:   compare[float32],
	less:      less[float32],
	order:     bits.OrderOfFloat32,
	min:       bits.MinFloat32,
	max:       bits.MaxFloat32,
	bounds:    bits.MinMaxFloat32,
	encode2:   encoding.Encoding.EncodeFloat,
	decode2:   encoding.Encoding.DecodeFloat,
}

var float64Class = class[float64]{
	name:      "DOUBLE",
	bits:      64,
	kind:      Double,
	makeValue: makeValueDouble,
	value:     Value.Double,
	compare:   compare[float64],
	less:      less[float64],
	order:     bits.OrderOfFloat64,
	min:       bits.MinFloat64,
	max:       bits.MaxFloat64,
	bounds:    bits.MinMaxFloat64,
	encode2:   encoding.Encoding.EncodeDouble,
	decode2:   encoding.Encoding.DecodeDouble,
}

var uint32Class = class[uint32]{
	name:      "INT32",
	bits:      32,
	kind:      Int32,
	makeValue: func(v uint32) Value { return makeValueInt32(int32(v)) },
	value:     func(v Value) uint32 { return uint32(v.Int32()) },
	compare:   compare[uint32],
	less:      less[uint32],
	order:     bits.OrderOfUint32,
	min:       bits.MinUint32,
	max:       bits.MaxUint32,
	bounds:    bits.MinMaxUint32,
	encode2:   encoding.Encoding.EncodeInt32,
	decode2:   encoding.Encoding.DecodeInt32,
}

var uint64Class = class[uint64]{
	name:      "INT64",
	bits:      64,
	kind:      Int64,
	makeValue: func(v uint64) Value { return makeValueInt64(int64(v)) },
	value:     func(v Value) uint64 { return uint64(v.Int64()) },
	compare:   compare[uint64],
	less:      less[uint64],
	order:     bits.OrderOfUint64,
	min:       bits.MinUint64,
	max:       bits.MaxUint64,
	bounds:    bits.MinMaxUint64,
	encode2:   encoding.Encoding.EncodeInt64,
	decode2:   encoding.Encoding.DecodeInt64,
}
