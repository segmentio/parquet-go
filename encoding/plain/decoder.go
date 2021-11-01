package plain

import (
	"encoding/binary"
	"fmt"
	"io"
)

type decoder struct {
	r io.Reader
	b [4]byte
}

func (d *decoder) Reset(r io.Reader) {
	d.r = r
}

func (d *decoder) DecodeBoolean(data []bool) (int, error) { return 0, io.EOF }

func (d *decoder) DecodeInt32(data []int32) (int, error) {
	return readFull(d.r, 4, unsafeInt32ToBytes(data))
}

func (d *decoder) DecodeInt64(data []int64) (int, error) {
	return readFull(d.r, 8, unsafeInt64ToBytes(data))
}

func (d *decoder) DecodeInt96(data [][12]byte) (int, error) {
	return readFull(d.r, 12, unsafeInt96ToBytes(data))
}

func (d *decoder) DecodeFloat(data []float32) (int, error) {
	return readFull(d.r, 4, unsafeFloat32ToBytes(data))
}

func (d *decoder) DecodeDouble(data []float64) (int, error) {
	return readFull(d.r, 8, unsafeFloat64ToBytes(data))
}

func (d *decoder) DecodeByteArray(data [][]byte) (int, error) {
	for i := range data {
		if _, err := io.ReadFull(d.r, d.b[:]); err != nil {
			return i, err
		}

		size := int(binary.LittleEndian.Uint32(d.b[:]))
		item := make([]byte, size)

		_, err := io.ReadFull(d.r, item)
		if err != nil {
			return i, err
		}

		data[i] = item
	}
	return len(data), nil
}

func (d *decoder) DecodeFixedLenByteArray(size int, data []byte) (int, error) {
	if (len(data) % size) != 0 {
		return 0, fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	return readFull(d.r, size, data)
}

func readFull(r io.Reader, scale int, data []byte) (int, error) {
	n, err := io.ReadFull(r, data)
	if err == io.ErrUnexpectedEOF && (n%scale) == 0 {
		err = io.EOF
	}
	return n / scale, err
}
