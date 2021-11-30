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
	reader io.Reader
	buffer []byte
	rle    *rle.Decoder
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{reader: r}
}

func (d *Decoder) Encoding() format.Encoding {
	return format.Plain
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

func (e *Decoder) DecodeInt8(data []int8) (int, error) {
	return 0, encoding.NotImplementedError("INT8")
}

func (e *Decoder) DecodeInt16(data []int16) (int, error) {
	return 0, encoding.NotImplementedError("INT16")
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

func (d *Decoder) DecodeByteArray(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if len(data) < 4 {
		return 0, encoding.ErrBufferTooShort
	}

	n := copy(data, d.buffer)
	d.buffer = d.buffer[:copy(d.buffer, d.buffer[n:])]

	if n < len(data) {
		r, err := io.ReadFull(d.reader, data[n:])
		if err != nil && (n+r) == 0 {
			return 0, err
		}
		data = data[:n+r]
	}

	if len(data) < 4 {
		d.buffer = prepend(d.buffer, data)
		return 0, io.ErrUnexpectedEOF
	}

	if size := int(binary.LittleEndian.Uint32(data)); size > (len(data) - 4) {
		d.buffer = prepend(d.buffer, data)
		return 0, encoding.ErrValueTooLarge
	}

	numValues := 0
	offset := uint(4)
	length := uint(0)

	for offset <= uint(len(data)) {
		length = uint(binary.LittleEndian.Uint32(data[offset-4:]))
		if length > (uint(len(data)) - offset) {
			break
		}
		numValues++
		offset += 4 + length
	}

	d.buffer = prepend(d.buffer, data[offset-4:])
	return numValues, nil
}

func (d *Decoder) DecodeFixedLenByteArray(size int, data []byte) (int, error) {
	if (len(data) % size) != 0 {
		return 0, fmt.Errorf("length of fixed byte array is not a multiple of its size: size=%d length=%d", size, len(data))
	}
	return readFull(d.reader, size, data)
}

func (d *Decoder) SetBitWidth(bitWidth int) {}

func readFull(r io.Reader, scale int, data []byte) (int, error) {
	n, err := io.ReadFull(r, data)
	if err == io.ErrUnexpectedEOF && (n%scale) == 0 {
		err = io.EOF
	}
	return n / scale, err
}

func prepend(dst, src []byte) (ret []byte) {
	if (cap(dst) - len(dst)) < len(src) {
		ret = make([]byte, len(src)+len(dst))
	} else {
		ret = dst[:len(src)+len(dst)]
	}
	copy(ret[len(src):], dst)
	copy(ret, src)
	return ret
}
