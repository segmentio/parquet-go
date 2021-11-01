package plain

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type encoder struct {
	w io.Writer
	b [4]byte
}

func (e *encoder) Reset(w io.Writer) {
	e.w = w
}

func (e *encoder) EncodeBoolean(data []bool) error {
	return nil
}

func (e *encoder) EncodeInt32(data []int32) error {
	_, err := e.w.Write(unsafeInt32ToBytes(data))
	return err
}

func (e *encoder) EncodeInt64(data []int64) error {
	_, err := e.w.Write(unsafeInt64ToBytes(data))
	return err
}

func (e *encoder) EncodeInt96(data [][12]byte) error {
	_, err := e.w.Write(unsafeInt96ToBytes(data))
	return err
}

func (e *encoder) EncodeFloat(data []float32) error {
	_, err := e.w.Write(unsafeFloat32ToBytes(data))
	return err
}

func (e *encoder) EncodeDouble(data []float64) error {
	_, err := e.w.Write(unsafeFloat64ToBytes(data))
	return err
}

func (e *encoder) EncodeByteArray(data [][]byte) error {
	for _, b := range data {
		if len(b) > math.MaxUint32 {
			return fmt.Errorf("byte slice is too large to be represented by the PLAIN encoding: %d", len(b))
		}
		binary.LittleEndian.PutUint32(e.b[:], uint32(len(b)))
		if _, err := e.w.Write(e.b[:]); err != nil {
			return err
		}
		if _, err := e.w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (e *encoder) EncodeFixedLenByteArray(size int, data []byte) error {
	if (len(data) % size) != 0 {
		return fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	_, err := e.w.Write(data)
	return err
}
