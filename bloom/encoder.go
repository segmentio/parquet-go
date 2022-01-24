package bloom

import (
	"io"

	"github.com/cespare/xxhash/v2"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/internal/bits"
)

// Encoder is an adapter type which implements the encoding.Encoder interface on
// top of a bloom filter.
type Encoder struct {
	Filter SplitBlockFilter
}

func (e *Encoder) Reset(io.Writer) {
	e.Filter.Reset()
}

func (e *Encoder) SetBitWidth(int) {
}

func (e *Encoder) EncodeBoolean(data []bool) error {
	return e.EncodeFixedLenByteArray(1, bits.BoolToBytes(data))
}

func (e *Encoder) EncodeInt8(data []int8) error {
	return e.EncodeFixedLenByteArray(1, bits.Int8ToBytes(data))
}

func (e *Encoder) EncodeInt16(data []int16) error {
	return e.EncodeFixedLenByteArray(2, bits.Int16ToBytes(data))
}

func (e *Encoder) EncodeInt32(data []int32) error {
	return e.EncodeFixedLenByteArray(4, bits.Int32ToBytes(data))
}

func (e *Encoder) EncodeInt64(data []int64) error {
	return e.EncodeFixedLenByteArray(8, bits.Int64ToBytes(data))
}

func (e *Encoder) EncodeInt96(data []deprecated.Int96) error {
	return e.EncodeFixedLenByteArray(12, deprecated.Int96ToBytes(data))
}

func (e *Encoder) EncodeFloat(data []float32) error {
	return e.EncodeFixedLenByteArray(4, bits.Float32ToBytes(data))
}

func (e *Encoder) EncodeDouble(data []float64) error {
	return e.EncodeFixedLenByteArray(8, bits.Float64ToBytes(data))
}

func (e *Encoder) EncodeByteArray(data encoding.ByteArrayList) error {
	data.Range(func(v []byte) bool { e.Filter.Insert(xxhash.Sum64(v)); return true })
	return nil
}

func (e *Encoder) EncodeFixedLenByteArray(size int, data []byte) error {
	for i, j := 0, size; j <= len(data); {
		e.Filter.Insert(xxhash.Sum64(data[i:j]))
		i += size
		j += size
	}
	return nil
}

var (
	_ encoding.Encoder = (*Encoder)(nil)
)
