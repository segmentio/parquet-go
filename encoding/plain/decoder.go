package plain

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/internal/bits"
)

type Decoder struct {
	r   io.Reader
	b   [4]byte
	rle *rle.Decoder
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

func (d *Decoder) Close() error {
	if d.rle != nil {
		return d.rle.Close()
	}
	return nil
}

func (d *Decoder) Reset(r io.Reader) {
	d.r = r

	if d.rle != nil {
		d.rle.Reset(r)
	}
}

func (d *Decoder) DecodeBoolean(data []bool) (int, error) {
	if d.rle == nil {
		d.rle = rle.NewDecoder(d.r)
	}
	return d.rle.DecodeBoolean(data)
}

func (d *Decoder) DecodeInt32(data []int32) (int, error) {
	return readFull(d.r, 4, bits.Int32ToBytes(data))
}

func (d *Decoder) DecodeInt64(data []int64) (int, error) {
	return readFull(d.r, 8, bits.Int64ToBytes(data))
}

func (d *Decoder) DecodeInt96(data [][12]byte) (int, error) {
	return readFull(d.r, 12, bits.Int96ToBytes(data))
}

func (d *Decoder) DecodeFloat(data []float32) (int, error) {
	return readFull(d.r, 4, bits.Float32ToBytes(data))
}

func (d *Decoder) DecodeDouble(data []float64) (int, error) {
	return readFull(d.r, 8, bits.Float64ToBytes(data))
}

func (d *Decoder) DecodeByteArray(data [][]byte) (int, error) {
	for i := range data {
		if n, err := io.ReadFull(d.r, d.b[:4]); err != nil {
			if err != io.EOF {
				err = fmt.Errorf("reading 4 bytes length prefix of PLAIN byte array: %w (%d bytes were read: %08b)", err, n, d.b[:n])
			}
			return i, err
		}

		size := int(binary.LittleEndian.Uint32(d.b[:4]))
		item := make([]byte, size)

		if size != 0 {
			_, err := io.ReadFull(d.r, item)
			if err != nil {
				return i, fmt.Errorf("reading value of PLAIN byte array of length %d: %w", size, err)
			}
		}

		data[i] = item

		if size > 0 {
			//			fmt.Printf("(%d) %q\n", len(item), item)
		}
	}
	return len(data), nil
}

func (d *Decoder) DecodeFixedLenByteArray(size int, data []byte) (int, error) {
	if (len(data) % size) != 0 {
		return 0, fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	return readFull(d.r, size, data)
}

func (d *Decoder) SetBitWidth(bitWidth int) {
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
