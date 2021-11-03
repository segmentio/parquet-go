package plain

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet/internal/bits"
)

type primitiveDecoder struct {
	r io.Reader
}

func (d *primitiveDecoder) Close() error {
	return nil
}

func (d *primitiveDecoder) Reset(r io.Reader) {
	d.r = r
}

func (d *primitiveDecoder) DecodeInt32(data []int32) (int, error) {
	return readFull(d.r, 4, bits.Int32ToBytes(data))
}

func (d *primitiveDecoder) DecodeInt64(data []int64) (int, error) {
	return readFull(d.r, 8, bits.Int64ToBytes(data))
}

func (d *primitiveDecoder) DecodeInt96(data [][12]byte) (int, error) {
	return readFull(d.r, 12, bits.Int96ToBytes(data))
}

func (d *primitiveDecoder) DecodeFloat(data []float32) (int, error) {
	return readFull(d.r, 4, bits.Float32ToBytes(data))
}

func (d *primitiveDecoder) DecodeDouble(data []float64) (int, error) {
	return readFull(d.r, 8, bits.Float64ToBytes(data))
}

func (d *primitiveDecoder) DecodeFixedLenByteArray(size int, data []byte) (int, error) {
	if (len(data) % size) != 0 {
		return 0, fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	return readFull(d.r, size, data)
}

func (d *primitiveDecoder) SetBitWidth(bitWidth int) {
	// The plain encoding does not vary based on the bit-width of values,
	// which is why this method does nothing.
}

func readFull(r io.Reader, scale int, data []byte) (int, error) {
	n, err := io.ReadFull(r, data)
	if err == io.ErrUnexpectedEOF && (n%scale) == 0 {
		err = io.EOF
	}
	return n / scale, err
}

type byteArrayDecoder struct {
	r io.Reader
	b [4]byte
}

func (d *byteArrayDecoder) Close() error {
	return nil
}

func (d *byteArrayDecoder) Reset(r io.Reader) {
	d.r = r
}

func (d *byteArrayDecoder) DecodeByteArray(data [][]byte) (int, error) {
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
