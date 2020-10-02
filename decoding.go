package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	pthrift "github.com/segmentio/centrifuge-traces/parquet/internal/gen-go/parquet"
)

var overflow = errors.New("binary: varint overflows a 64-bit integer")

// readUvarint is the same as binary.ReadUvarint except it returns the number
// of bytes that it read.
func readUvarint(r io.Reader) (uint64, uint32, error) {
	var x uint64
	var s uint
	var read uint32
	buff := []byte{0}
	for i := 0; ; i++ {
		_, err := r.Read(buff)
		if err != nil {
			return x, read, err
		}
		b := buff[0]
		read++
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return x, read, overflow
			}
			return x | uint64(b)<<s, read, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

type Decoder interface {
	prepare(r io.Reader)
	Int32() (int32, error)
	Int64() (int64, error)
	Uint32(bitWidth int, out []uint32) error
	ByteArray(dst []byte) ([]byte, error)
}

type EmptyDecoder struct{}

func (e *EmptyDecoder) prepare(r io.Reader)                     {}
func (e *EmptyDecoder) Int32() (int32, error)                   { return 0, nil }
func (e *EmptyDecoder) Int64() (int64, error)                   { return 0, nil }
func (e *EmptyDecoder) Uint32(bitWidth int, out []uint32) error { return nil }
func (e *EmptyDecoder) ByteArray() ([]byte, error)              { return nil, nil }

// Construct a Decoder for a given encoding.
// Technically, not all encoding work everywhere. Let's say good-enough for now.
func decoderFor(enc pthrift.Encoding) (Decoder, error) {
	switch enc {
	case pthrift.Encoding_PLAIN:
		return &plainDecoder{}, nil
	case pthrift.Encoding_RLE:
		return &rleDecoder{}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", enc)
	}
}

type rleDecoder struct {
	r       io.Reader
	buff    [4]byte
	scratch []byte
}

func (d *rleDecoder) prepare(r io.Reader) {
	d.r = r
}

// https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
// This implementation only handles uint32 values. This is because the spec has
// > RLE encoding method is only supported for the following types of data:
// >  Repetition and definition levels
// >  Dictionary indices
// >  Boolean values in data pages
// All those values fit in uint32.
func (d *rleDecoder) Uint32(bitWidth int, out []uint32) error {
	b := d.buff[:]
	_, err := d.r.Read(b)
	if err != nil {
		return err
	}

	size := binary.LittleEndian.Uint32(b)
	read := uint32(0)
	idx := 0

	for read < size {
		length, n, err := readUvarint(d.r)
		if err != nil {
			return fmt.Errorf("cannot read uvarint: %w", err)
		}
		read += n

		runLength := length >> 1

		if length&1 == 1 {
			// TODO: bit-packed run
			// Not implemented because parquet-go takes the shortcut of never
			// generating bit-packed runs, always RLE runs.
			panic("bit-packed in RLE decoding not implemented")
		} else {
			// rle run
			bytesInRLE := (bitWidth + 7) / 8
			bytesInBuffer := bytesInRLE
			if bytesInBuffer < 4 {
				bytesInBuffer = 4
			}

			if len(d.scratch) < bytesInBuffer {
				v := bytesInBuffer
				v--
				v |= v >> 1
				v |= v >> 2
				v |= v >> 4
				v |= v >> 8
				v |= v >> 16
				v++
				d.scratch = make([]byte, v)
			}

			data := d.scratch[:bytesInRLE]
			if bytesInRLE > 0 {
				n, err := d.r.Read(data)
				if err != nil {
					return err
				}
				read += uint32(n)
			}
			for len(data) < 4 {
				data = append(data, 0x0)
			}
			val := binary.LittleEndian.Uint32(data)
			for i := uint64(0); i < runLength; i++ {
				out[idx] = val
				idx++
			}
		}
	}

	return nil
}

func (d *rleDecoder) Int32() (int32, error) {
	panic("rleDecoder does not implement Int32")
}

func (d *rleDecoder) Int64() (int64, error) {
	panic("rleDecoder does not implement Int64")
}

func (d *rleDecoder) ByteArray(dst []byte) ([]byte, error) {
	panic("rleDecoder does not implement ByteArray")
}

type plainDecoder struct {
	r    io.Reader
	buff [8]byte
}

func (d *plainDecoder) prepare(r io.Reader) {
	d.r = r
}

func (d *plainDecoder) Int32() (int32, error) {
	_, err := d.r.Read(d.buff[:4])
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(d.buff[:4])), nil
}

func (d *plainDecoder) Int64() (int64, error) {
	_, err := d.r.Read(d.buff[:8])
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(d.buff[:8])), nil
}

func (d *plainDecoder) Uint32(bitWidth int, out []uint32) error {
	panic("plainDecoder does not implement Uint32")
}

func (d *plainDecoder) ByteArray(dst []byte) ([]byte, error) {
	_, err := d.r.Read(d.buff[:4])
	if err != nil {
		return nil, fmt.Errorf("cannot read byte array size: %w", err)
	}
	size := binary.LittleEndian.Uint32(d.buff[:4])
	if cap(dst) < int(size) {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	if size > 0 {
		var n int
		n, err = d.r.Read(dst)
		if err != nil {
			return nil, err
		}
		if uint32(n) != size {
			panic("read should have returned the right amount of data")
		}
	}
	return dst, nil
}
