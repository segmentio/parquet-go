package plain

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/internal/bits"
)

type Encoder struct {
	w   io.Writer
	b   [4]byte
	rle *rle.Encoder
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

func (e *Encoder) Close() error {
	if e.rle != nil {
		return e.rle.Close()
	}
	return nil
}

func (e *Encoder) Reset(w io.Writer) {
	e.w = w

	if e.rle != nil {
		e.rle.Reset(w)
	}
}

func (e *Encoder) EncodeBoolean(data []bool) error {
	if e.rle == nil {
		e.rle = rle.NewEncoder(e.w)
	}
	return e.rle.EncodeBoolean(data)
}

func (e *Encoder) EncodeInt32(data []int32) error {
	_, err := e.w.Write(bits.Int32ToBytes(data))
	return err
}

func (e *Encoder) EncodeInt64(data []int64) error {
	_, err := e.w.Write(bits.Int64ToBytes(data))
	return err
}

func (e *Encoder) EncodeInt96(data [][12]byte) error {
	_, err := e.w.Write(bits.Int96ToBytes(data))
	return err
}

func (e *Encoder) EncodeFloat(data []float32) error {
	_, err := e.w.Write(bits.Float32ToBytes(data))
	return err
}

func (e *Encoder) EncodeDouble(data []float64) error {
	_, err := e.w.Write(bits.Float64ToBytes(data))
	return err
}

func (e *Encoder) EncodeByteArray(data [][]byte) error {
	for _, b := range data {
		if len(b) > math.MaxUint32 {
			return fmt.Errorf("byte slice is too large to be represented by the PLAIN encoding: %d", len(b))
		}
		binary.LittleEndian.PutUint32(e.b[:4], uint32(len(b)))
		if _, err := e.w.Write(e.b[:4]); err != nil {
			return err
		}
		if _, err := e.w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) EncodeFixedLenByteArray(size int, data []byte) error {
	if (len(data) % size) != 0 {
		return fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	_, err := e.w.Write(data)
	return err
}

func (e *Encoder) SetBitWidth(int) {
	// see (*Decoder).SetBitWidth
}
