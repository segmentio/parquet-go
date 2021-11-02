package plain

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type primitiveEncoder struct {
	w io.Writer
}

func (e *primitiveEncoder) Close() error {
	return nil
}

func (e *primitiveEncoder) Reset(w io.Writer) {
	e.w = w
}

func (e *primitiveEncoder) EncodeInt32(data []int32) error {
	_, err := e.w.Write(unsafeInt32ToBytes(data))
	return err
}

func (e *primitiveEncoder) EncodeInt64(data []int64) error {
	_, err := e.w.Write(unsafeInt64ToBytes(data))
	return err
}

func (e *primitiveEncoder) EncodeInt96(data [][12]byte) error {
	_, err := e.w.Write(unsafeInt96ToBytes(data))
	return err
}

func (e *primitiveEncoder) EncodeFloat(data []float32) error {
	_, err := e.w.Write(unsafeFloat32ToBytes(data))
	return err
}

func (e *primitiveEncoder) EncodeDouble(data []float64) error {
	_, err := e.w.Write(unsafeFloat64ToBytes(data))
	return err
}

func (e *primitiveEncoder) EncodeFixedLenByteArray(size int, data []byte) error {
	if (len(data) % size) != 0 {
		return fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	_, err := e.w.Write(data)
	return err
}

type byteArrayEncoder struct {
	w io.Writer
	b [4]byte
}

func (e *byteArrayEncoder) Close() error {
	return nil
}

func (e *byteArrayEncoder) Reset(w io.Writer) {
	e.w = w
}

func (e *byteArrayEncoder) EncodeByteArray(data [][]byte) error {
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
