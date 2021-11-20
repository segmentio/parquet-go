package plain

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/encoding/rle"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type Decoder struct {
	reader   io.Reader
	buffer   []byte
	rle      *rle.Decoder
	bitWidth int
}

func NewDecoder(r io.Reader) *Decoder {
	return NewDecoderSize(r, encoding.DefaultBufferSize)
}

func NewDecoderSize(r io.Reader, bufferSize int) *Decoder {
	return &Decoder{
		reader: r,
		buffer: make([]byte, bits.NearestPowerOfTwo64(uint64(bufferSize))),
	}
}

func (d *Decoder) Encoding() format.Encoding {
	return format.Plain
}

func (d *Decoder) Close() error {
	if d.rle != nil {
		return d.rle.Close()
	}
	return nil
}

func (d *Decoder) Reset(r io.Reader) {
	d.reader = r

	if d.rle != nil {
		d.rle.Reset(r)
	}
}

func (d *Decoder) DecodeBoolean(data []bool) (int, error) {
	if d.rle == nil {
		d.rle = rle.NewDecoder(d.reader)
	}
	return d.rle.DecodeBoolean(data)
}

func (d *Decoder) DecodeInt32(data []int32) (int, error) {
	return readFull(d.reader, 4, bits.Int32ToBytes(data))
}

func (d *Decoder) DecodeInt64(data []int64) (int, error) {
	return readFull(d.reader, 8, bits.Int64ToBytes(data))
}

func (d *Decoder) DecodeInt96(data [][12]byte) (int, error) {
	return readFull(d.reader, 12, bits.Int96ToBytes(data))
}

func (d *Decoder) DecodeFloat(data []float32) (int, error) {
	return readFull(d.reader, 4, bits.Float32ToBytes(data))
}

func (d *Decoder) DecodeDouble(data []float64) (int, error) {
	return readFull(d.reader, 8, bits.Float64ToBytes(data))
}

func (d *Decoder) DecodeByteArray(data [][]byte) (int, error) {
	for i := range data {
		if n, err := io.ReadFull(d.reader, d.buffer[:4]); err != nil {
			if err != io.EOF {
				err = fmt.Errorf("reading 4 bytes length prefix of PLAIN byte array: %w (%d bytes were read: %08b)", err, n, d.buffer[:n])
			}
			return i, err
		}

		size := int(binary.LittleEndian.Uint32(d.buffer[:4]))
		item := make([]byte, size)

		if size != 0 {
			_, err := io.ReadFull(d.reader, item)
			if err != nil {
				return i, fmt.Errorf("reading value of PLAIN byte array of length %d: %w", size, err)
			}
		}

		data[i] = item
	}
	return len(data), nil
}

func (d *Decoder) DecodeFixedLenByteArray(size int, data []byte) (int, error) {
	if (len(data) % size) != 0 {
		return 0, fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	return readFull(d.reader, size, data)
}

func (d *Decoder) DecodeIntArray(data encoding.IntArrayBuffer) error {
	if d.bitWidth == 0 {
		return fmt.Errorf("bit width must be set on PLAIN decoder before reading values into a dynamic int array")
	}
	scale := d.bitWidth / 8
	for {
		n, err := readFull(d.reader, scale, d.buffer)
		if n > 0 {
			data.AppendBits(d.buffer[:n*scale], d.bitWidth)
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
	}
}

func (d *Decoder) SetBitWidth(bitWidth int) {
	d.bitWidth = coerceBitWidth(bitWidth)
}

func readFull(r io.Reader, scale int, data []byte) (int, error) {
	n, err := io.ReadFull(r, data)
	if err == io.ErrUnexpectedEOF && (n%scale) == 0 {
		err = io.EOF
	}
	return n / scale, err
}
