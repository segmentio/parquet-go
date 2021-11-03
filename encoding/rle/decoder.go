package rle

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet/internal/bits"
)

type decoder struct {
	data io.LimitedReader
	init bool
	buf  [binary.MaxVarintLen32]byte
	dec  hybridDecoder
	run  runLengthDecoder
	bit  bitPackDecoder

	width    uint32
	bitWidth uint32
}

func newDecoder(r io.Reader) *decoder {
	return &decoder{data: io.LimitedReader{R: r, N: 4}}
}

func (d *decoder) Close() error {
	return nil
}

func (d *decoder) SetBitWidth(bitWidth int) {
	d.width = uint32(bitWidth+7) / 8
	d.bitWidth = uint32(bitWidth)
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
	return d.decode(len(data), 1, func(r io.Reader, offset, length int) (int, error) {
		return d.dec.decodeBoolean(r, data[offset:offset+length])
	})
}

func (d *decoder) DecodeInt32(data []int32) (int, error) {
	bitWidth := coalesceUint32(d.bitWidth, 32)
	return d.decode(len(data), bitWidth, func(r io.Reader, offset, length int) (int, error) {
		return d.dec.decodeInt32(r, bitWidth, data[offset:offset+length])
	})
}

func (d *decoder) DecodeInt64(data []int64) (int, error) {
	bitWidth := coalesceUint32(d.bitWidth, 64)
	return d.decode(len(data), bitWidth, func(r io.Reader, offset, length int) (int, error) {
		return d.dec.decodeInt64(r, bitWidth, data[offset:offset+length])
	})
}

func (d *decoder) DecodeInt96(data [][12]byte) (int, error) {
	bitWidth := coalesceUint32(d.bitWidth, 96)
	return d.decode(len(data), bitWidth, func(r io.Reader, offset, length int) (int, error) {
		return d.dec.decodeInt96(r, bitWidth, data[offset:offset+length])
	})
}

func (d *decoder) decode(length int, bitWidth uint32, decode func(r io.Reader, offset, length int) (int, error)) (int, error) {
	width := coalesceUint32(d.width, bitWidth/8)
	offset := 0

	if !d.init {
		_, err := io.ReadFull(&d.data, d.buf[:4])
		if err != nil {
			return 0, fmt.Errorf("decoding RLE length: %w", err)
		}
		d.data.N = int64(binary.LittleEndian.Uint32(d.buf[:4]))
		d.init = true
	}

	for length > 0 {
		if d.dec == nil {
			u, err := binary.ReadUvarint(d)
			if err != nil {
				if err != io.EOF {
					err = fmt.Errorf("decoding RLE run length: %w", err)
				}
				return offset, err
			}

			count, bitpack := uint32(u>>1), (u&1) != 0
			if bitpack {
				d.bit.remain = int(count * 8)
				d.bit.offset = 0
				d.bit.length = 0
				d.dec = &d.bit
			} else {
				d.run.count = count
				if count == 0 {
					d.run.value = [12]byte{}
				} else {
					_, err := io.ReadFull(&d.data, d.run.value[:width])
					if err != nil {
						return offset, fmt.Errorf("decoding RLE repeated value after count=%d: %w", count, err)
					}
				}
				d.dec = &d.run
			}
		}

		n, err := decode(&d.data, offset, length)
		if err != nil {
			if err == io.EOF {
				d.dec = nil
			} else {
				return offset + n, fmt.Errorf("decoding RLE values: %w", err)
			}
		}

		offset += n
		length -= n
	}

	return offset, nil
}

type hybridDecoder interface {
	decodeBoolean(io.Reader, []bool) (int, error)
	decodeInt32(io.Reader, uint32, []int32) (int, error)
	decodeInt64(io.Reader, uint32, []int64) (int, error)
	decodeInt96(io.Reader, uint32, [][12]byte) (int, error)
}

// large enougth to contain 8 x int96
const bitBufferSize = 96

type bitPackDecoder struct {
	remain int
	offset int
	length int
	buffer [bitBufferSize]byte
}

func (d *bitPackDecoder) decodeBoolean(r io.Reader, data []bool) (int, error) {
	return d.decode(r, 1, 1, bits.BoolToBytes(data))
}

func (d *bitPackDecoder) decodeInt32(r io.Reader, bitWidth uint32, data []int32) (int, error) {
	return d.decode(r, bitWidth, 32, bits.Int32ToBytes(data))
}

func (d *bitPackDecoder) decodeInt64(r io.Reader, bitWidth uint32, data []int64) (int, error) {
	return d.decode(r, bitWidth, 64, bits.Int64ToBytes(data))
}

func (d *bitPackDecoder) decodeInt96(r io.Reader, bitWidth uint32, data [][12]byte) (int, error) {
	return d.decode(r, bitWidth, 96, bits.Int96ToBytes(data))
}

func (d *bitPackDecoder) decode(r io.Reader, bitWidth, wordWidth uint32, data []byte) (int, error) {
	if bitWidth == wordWidth {
		return io.ReadFull(r, data)
	}

	remained := d.remain
	for {
		if d.length == d.offset {
			// We know that bit-pack data is encoded in chunks of 8 since the
			// bit length is divided by 8. We look for the multiple of 8
			// to maximize buffer utilization.
			chunkSizeBits := 8 * uint(bitWidth)
			numberOfChunks := bits.BitCount(len(d.buffer)) / chunkSizeBits
			numberOfBytes := bits.ByteCount(numberOfChunks * chunkSizeBits)
			// We limit the read to the number of bytes that remain to be read
			// from the underlying io.Reader, otherwise we would read beyond the
			// end of the bit-packed run.
			remainingBytes := bits.ByteCount(uint(d.remain) * uint(bitWidth))
			if remainingBytes < numberOfBytes {
				numberOfBytes = remainingBytes
			}

			n, err := io.ReadFull(r, d.buffer[:numberOfBytes])
			if err != nil {
				return remained - d.remain, err
			}

			// At this point we have the guarantee that the number of bytes read
			// is a multiple of the bit-width, the data ends at the end of a
			// byte, there will not be partial trailing words.
			d.offset = 0
			d.length = 8 * n
		}

		unpacked := bits.Unpack(data, uint(wordWidth), d.buffer[d.offset:d.length], uint(bitWidth))

		if d.offset += unpacked * int(bitWidth); d.offset < d.length {
			bitShift := unpacked * int(bitWidth)
			bitCount := d.length - d.offset
			tmp := d.buffer                  // copy
			d.buffer = [bitBufferSize]byte{} // clear
			d.offset = 0
			d.length = bits.Copy(d.buffer[:], tmp[:], uint(bitShift), uint(bitCount))
		}

		d.remain -= unpacked
		data = data[:(unpacked*int(wordWidth))/8]

		if d.remain == 0 {
			return remained - d.remain, nil
		}
	}
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
	bits.Fill(bits.BoolToBytes(data), d.value[:1])
	d.count -= uint32(len(data))
	return len(data), nil
}

func (d *runLengthDecoder) decodeInt32(r io.Reader, bitWidth uint32, data []int32) (int, error) {
	if d.count == 0 {
		return 0, io.EOF
	}
	if len(data) > int(d.count) {
		data = data[:d.count]
	}
	bits.Fill(bits.Int32ToBytes(data), d.value[:4])
	d.count -= uint32(len(data))
	return len(data), nil
}

func (d *runLengthDecoder) decodeInt64(r io.Reader, bitWidth uint32, data []int64) (int, error) {
	if d.count == 0 {
		return 0, io.EOF
	}
	if len(data) > int(d.count) {
		data = data[:d.count]
	}
	bits.Fill(bits.Int64ToBytes(data), d.value[:8])
	d.count -= uint32(len(data))
	return len(data), nil
}

func (d *runLengthDecoder) decodeInt96(r io.Reader, bitWidth uint32, data [][12]byte) (int, error) {
	if d.count == 0 {
		return 0, io.EOF
	}
	if len(data) > int(d.count) {
		data = data[:d.count]
	}
	bits.Fill(bits.Int96ToBytes(data), d.value[:])
	d.count -= uint32(len(data))
	return len(data), nil
}
