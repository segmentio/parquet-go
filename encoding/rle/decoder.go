package rle

import (
	"encoding/binary"
	"io"

	"github.com/segmentio/parquet/encoding"
)

type decoder struct {
	data io.LimitedReader
	init bool
	buf  [binary.MaxVarintLen32]byte
	dec  hybridDecoder
	run  runLengthDecoder
	bit  bitPackDecoder
}

func newDecoder(r io.Reader) *decoder {
	return &decoder{data: io.LimitedReader{R: r, N: 4}}
}

func (d *decoder) Close() error {
	return nil
}

func (d *decoder) Reset(r io.Reader) {
	d.data.R = r
	d.data.N = 4
	d.init = false
	d.dec = nil
}

func (d *decoder) ReadByte() (byte, error) {
	_, err := d.data.Read(d.buf[:1])
	return d.buf[0], err
}

func (d *decoder) DecodeBoolean(data []bool) (int, error) {
	return d.decode(len(data), 8, func(r io.Reader, offset, length int) (int, error) {
		return d.dec.decodeBoolean(r, data[offset:offset+length])
	})
}

func (d *decoder) DecodeInt32(data []int32) (int, error) {
	return d.decode(len(data), 32, func(r io.Reader, offset, length int) (int, error) {
		return d.dec.decodeInt32(r, data[offset:offset+length])
	})
}

func (d *decoder) DecodeInt64(data []int64) (int, error) {
	return d.decode(len(data), 64, func(r io.Reader, offset, length int) (int, error) {
		return d.dec.decodeInt64(r, data[offset:offset+length])
	})
}

func (d *decoder) DecodeInt96(data [][12]byte) (int, error) {
	return d.decode(len(data), 96, func(r io.Reader, offset, length int) (int, error) {
		return d.dec.decodeInt96(r, data[offset:offset+length])
	})
}

func (d *decoder) decode(length, bitwidth int, decode func(r io.Reader, offset, length int) (int, error)) (int, error) {
	offset := 0

	if !d.init {
		_, err := io.ReadFull(&d.data, d.buf[:4])
		if err != nil {
			return 0, err
		}
		d.data.N = int64(binary.LittleEndian.Uint32(d.buf[:4]))
		d.init = true
	}

	for length > 0 {
		if d.dec == nil {
			u, err := binary.ReadUvarint(d)
			if err != nil {
				return offset, err
			}

			count, bitpack := uint32(u>>1), (u&1) != 0
			if bitpack {
				d.bit.remain = int(count)
				d.bit.offset = 0
				d.bit.length = 0
				d.dec = &d.bit
			} else {
				d.run.count = count
				_, err := io.ReadFull(&d.data, d.run.value[:bitwidth/8])
				if err != nil {
					return offset, err
				}
				d.dec = &d.run
			}
		}

		n, err := decode(&d.data, offset, length)
		if err != nil {
			if err == io.EOF {
				d.dec = nil
			} else {
				return offset + n, err
			}
		}

		offset += n
		length -= n
	}

	return offset, nil
}

type hybridDecoder interface {
	decodeBoolean(io.Reader, []bool) (int, error)
	decodeInt32(io.Reader, []int32) (int, error)
	decodeInt64(io.Reader, []int64) (int, error)
	decodeInt96(io.Reader, [][12]byte) (int, error)
}

type bitPackDecoder struct {
	remain int
	offset int
	length int
	buffer [64]byte
}

func (d *bitPackDecoder) decodeBoolean(r io.Reader, data []bool) (int, error) {
	count := 0

	for len(data) != 0 {
		if d.remain == 0 {
			return count, io.EOF
		}

		if d.offset == d.length {
			n, err := io.ReadFull(r, d.buffer[:])
			if err != nil && (n == 0 || (n%8) != 0) {
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				return 0, err
			}
			d.offset = 0
			d.length = n * 8
		}

		limit := d.remain
		avail := d.length - d.offset
		if avail < limit {
			limit = avail
		}
		if len(data) < limit {
			limit = len(data)
		}

		for i := range data[:limit] {
			j := d.offset + i
			x := j / 8
			y := j % 8
			data[i] = (d.buffer[x] & byte(1<<y)) != 0
		}

		d.remain -= limit
		d.offset += limit
		count += limit
		data = data[limit:]
	}

	return count, nil
}

func (d *bitPackDecoder) decodeInt32(r io.Reader, data []int32) (int, error) {
	return 0, encoding.NotImplementedError("INT32")
}

func (d *bitPackDecoder) decodeInt64(r io.Reader, data []int64) (int, error) {
	return 0, encoding.NotImplementedError("INT64")
}

func (d *bitPackDecoder) decodeInt96(r io.Reader, data [][12]byte) (int, error) {
	return 0, encoding.NotImplementedError("INT96")
}

type runLengthDecoder struct {
	count uint32
	value [12]byte
}

func (d *runLengthDecoder) decodeBoolean(r io.Reader, data []bool) (int, error) {
	if d.count == 0 {
		return 0, io.EOF
	}
	if len(data) > int(d.count) {
		data = data[:d.count]
	}
	v := d.value[0] != 0
	for i := range data {
		data[i] = v
	}
	d.count -= uint32(len(data))
	return len(data), nil
}

func (d *runLengthDecoder) decodeInt32(r io.Reader, data []int32) (int, error) {
	if d.count == 0 {
		return 0, io.EOF
	}
	if len(data) > int(d.count) {
		data = data[:d.count]
	}
	v := int32(binary.LittleEndian.Uint32(d.value[:4]))
	for i := range data {
		data[i] = v
	}
	d.count -= uint32(len(data))
	return len(data), nil
}

func (d *runLengthDecoder) decodeInt64(r io.Reader, data []int64) (int, error) {
	if d.count == 0 {
		return 0, io.EOF
	}
	if len(data) > int(d.count) {
		data = data[:d.count]
	}
	v := int64(binary.LittleEndian.Uint64(d.value[:8]))
	for i := range data {
		data[i] = v
	}
	d.count -= uint32(len(data))
	return len(data), nil
}

func (d *runLengthDecoder) decodeInt96(r io.Reader, data [][12]byte) (int, error) {
	if d.count == 0 {
		return 0, io.EOF
	}
	if len(data) > int(d.count) {
		data = data[:d.count]
	}
	for i := range data {
		data[i] = d.value
	}
	d.count -= uint32(len(data))
	return len(data), nil
}
