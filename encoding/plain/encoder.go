package plain

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/rle"
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

func (e *Encoder) Close() error {
	if e.rle != nil {
		return e.rle.Close()
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

func (e *Encoder) EncodeByteArray(data [][]byte) error {
	for _, b := range data {
		if len(b) > math.MaxUint32 {
			return fmt.Errorf("byte slice is too large to be represented by the PLAIN encoding: %d", len(b))
		}
		binary.LittleEndian.PutUint32(e.buffer[:4], uint32(len(b)))
		if _, err := e.writer.Write(e.buffer[:4]); err != nil {
			return err
		}
		if _, err := e.writer.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) EncodeFixedLenByteArray(size int, data []byte) error {
	if (len(data) % size) != 0 {
		return fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	_, err := e.writer.Write(data)
	return err
}

func (e *Encoder) EncodeIntArray(data encoding.IntArrayView) (err error) {
	srcWidth := data.BitWidth()
	dstWidth := e.bitWidth
	if dstWidth == 0 {
		dstWidth = coerceBitWidth(srcWidth)
	}

	if dstWidth == srcWidth {
		// The widths match, we can do a simple copy from the input buffer
		// to the output.
		_, err = e.writer.Write(data.Bits())
	} else {
		// TODO:
		// * use a wider buffer to avoid calling Write for every value
		// * expand bits from the input array to the output buffer
		numWords := data.Len()
		wordSize := dstWidth / 8 // 4 or 8
		wordData := e.buffer[:wordSize]

		if dstWidth == 64 {
			for i := 0; i < numWords && err == nil; i++ {
				binary.LittleEndian.PutUint64(wordData, uint64(data.Index(i)))
				_, err = e.writer.Write(wordData)
			}
		} else {
			for i := 0; i < numWords && err == nil; i++ {
				binary.LittleEndian.PutUint32(wordData, uint32(data.Index(i)))
				_, err = e.writer.Write(wordData)
			}
		}
	}

	return err
}

func (e *Encoder) SetBitWidth(bitWidth int) {
	e.bitWidth = coerceBitWidth(bitWidth)
}
