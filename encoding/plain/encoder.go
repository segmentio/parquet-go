package plain

import (
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type Encoder struct {
	writer   io.Writer
	buffer   [8]byte
	rle      *rle.Encoder
	bitWidth int
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{writer: w}
}

func (e *Encoder) Encoding() format.Encoding {
	return format.Plain
}

func (e *Encoder) Flush() error {
	if e.rle != nil {
		return e.rle.Flush()
	}
	return nil
}

func (e *Encoder) Reset(w io.Writer) {
	e.writer = w

	if e.rle != nil {
		e.rle.Reset(w)
	}
}

func (e *Encoder) EncodeBoolean(data []bool) error {
	if e.rle == nil {
		e.rle = rle.NewEncoder(e.writer)
	}
	return e.rle.EncodeBoolean(data)
}

func (e *Encoder) EncodeInt8(data []int8) error {
	return encoding.NotImplementedError("INT8")
}

func (e *Encoder) EncodeInt16(data []int16) error {
	return encoding.NotImplementedError("INT16")
}

func (e *Encoder) EncodeInt32(data []int32) error {
	_, err := e.writer.Write(bits.Int32ToBytes(data))
	return err
}

func (e *Encoder) EncodeInt64(data []int64) error {
	_, err := e.writer.Write(bits.Int64ToBytes(data))
	return err
}

func (e *Encoder) EncodeInt96(data [][12]byte) error {
	_, err := e.writer.Write(bits.Int96ToBytes(data))
	return err
}

func (e *Encoder) EncodeFloat(data []float32) error {
	_, err := e.writer.Write(bits.Float32ToBytes(data))
	return err
}

func (e *Encoder) EncodeDouble(data []float64) error {
	_, err := e.writer.Write(bits.Float64ToBytes(data))
	return err
}

func (e *Encoder) EncodeByteArray(data []byte) error {
	if _, err := ScanByteArrayList(data, len(data)/4, func(value []byte) error {
		return nil
	}); err != nil {
		return err
	}
	_, err := e.writer.Write(data)
	return err
}

func (e *Encoder) EncodeFixedLenByteArray(size int, data []byte) error {
	if (len(data) % size) != 0 {
		return fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	_, err := e.writer.Write(data)
	return err
}

func (e *Encoder) SetBitWidth(bitWidth int) {
	e.bitWidth = coerceBitWidth(bitWidth)
}
