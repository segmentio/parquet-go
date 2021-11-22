package rle

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/segmentio/parquet/encoding"
	"github.com/segmentio/parquet/format"
	"github.com/segmentio/parquet/internal/bits"
)

type Decoder struct {
	encoding.NotImplementedDecoder
	data      io.LimitedReader
	init      bool
	buffer    []byte
	bitWidth  uint
	decoder   hybridDecoder
	runLength runLengthDecoder
	bitPack   bitPackDecoder
}

func NewDecoder(r io.Reader) *Decoder {
	return NewDecoderSize(r, encoding.DefaultBufferSize)
}

func NewDecoderSize(r io.Reader, bufferSize int) *Decoder {
	return &Decoder{
		data: io.LimitedReader{
			R: r,
			N: math.MaxInt64,
		},
		buffer: make([]byte, bits.NearestPowerOfTwo64(uint64(bufferSize))),
	}
}

func (d *Decoder) Encoding() format.Encoding {
	return format.RLE
}

func (d *Decoder) Close() error {
	return nil
}

func (d *Decoder) BitWidth() int {
	return int(d.bitWidth)
}

func (d *Decoder) SetBitWidth(bitWidth int) {
	d.bitWidth = uint(bitWidth)
}

func (d *Decoder) Reset(r io.Reader) {
	d.data.R = r
	d.data.N = math.MaxInt64
	d.init = false
	d.decoder = nil
}

func (d *Decoder) ReadByte() (byte, error) {
	_, err := d.data.Read(d.buffer[:1])
	return d.buffer[0], err
}

func (d *Decoder) DecodeBoolean(data []bool) (int, error) {
	return d.decode(bits.BoolToBytes(data), 8, 1)
}

func (d *Decoder) DecodeInt8(data []int8) (int, error) {
	return d.decode(bits.Int8ToBytes(data), 8, d.bitWidth)
}

func (d *Decoder) DecodeInt16(data []int16) (int, error) {
	return d.decode(bits.Int16ToBytes(data), 16, d.bitWidth)
}

func (d *Decoder) DecodeInt32(data []int32) (int, error) {
	return d.decode(bits.Int32ToBytes(data), 32, d.bitWidth)
}

func (d *Decoder) DecodeInt64(data []int64) (int, error) {
	return d.decode(bits.Int64ToBytes(data), 64, d.bitWidth)
}

func (d *Decoder) DecodeInt96(data [][12]byte) (int, error) {
	return d.decode(bits.Int96ToBytes(data), 96, d.bitWidth)
}

func (d *Decoder) decode(data []byte, dstWidth, srcWidth uint) (int, error) {
	wordSize := bits.ByteCount(dstWidth)
	decoded := 0
	if srcWidth == 0 {
		srcWidth = dstWidth
	}

	if !d.init {
		_, err := io.ReadFull(&d.data, d.buffer[:4])
		if err != nil {
			if err != io.EOF {
				err = fmt.Errorf("decoding RLE length: %w", err)
			}
			return 0, err
		}
		d.data.N = int64(binary.LittleEndian.Uint32(d.buffer[:4]))
		d.init = true
	}

	for len(data) > 0 {
		if d.decoder == nil {
			u, err := binary.ReadUvarint(d)
			if err != nil {
				if err != io.EOF {
					err = fmt.Errorf("decoding RLE run length: %w", err)
				}
				return decoded, err
			}

			count, bitpack := uint32(u>>1), (u&1) != 0
			if bitpack {
				d.bitPack.remain = int(count * 8)
				d.bitPack.offset = 0
				d.bitPack.length = 0
				d.decoder = &d.bitPack
			} else {
				d.runLength.count = count
				if count == 0 {
					d.runLength.value = [12]byte{}
				} else {
					length := bits.ByteCount(srcWidth)
					_, err := io.ReadFull(&d.data, d.runLength.value[:length])
					if err != nil {
						return decoded, fmt.Errorf("decoding RLE repeated value of length %d after count=%d: %w", length, count, err)
					}
				}
				d.decoder = &d.runLength
			}
		}

		n, err := d.decoder.decode(&d.data, data, dstWidth, srcWidth)
		decoded += n / wordSize

		if err != nil {
			if err == io.EOF {
				d.decoder = nil
			} else {
				return decoded, fmt.Errorf("decoding RLE values from %s encoded run: %w", d.decoder, err)
			}
		}

		data = data[n:]
	}

	return decoded, nil
}

type hybridDecoder interface {
	decode(io.Reader, []byte, uint, uint) (int, error)
}

// large enougth to contain 8 x int96
const bitBufferSize = 96

type bitPackDecoder struct {
	// Number of values that remain to be deocded, this field is initialized by
	// the decoder from reading the bit-pack header.
	remain int
	// Offset of in the bit-pack buffer of the first byte that contains data
	// loaded from the io.Reader.
	offset int
	// Number of bytes loaded from the io.Reader; starts at the beginning of the
	// buffer (the byte at index 0).
	length int
	// Buffer where bits are loaded from the io.Reader; the size is large enough
	// to contain 8 int96 values, since bit-pack integers are encoded in chunks
	// of 8 values. Most of the time the source bit-width is a lot smaller than
	// 96 bits (commonly 2-3 bits), so a lot more values get loaded in each read
	// from the io.Reader.
	buffer [bitBufferSize]byte
}

func (d *bitPackDecoder) String() string { return "BIT_PACK" }

func (d *bitPackDecoder) decode(r io.Reader, data []byte, dstWidth, srcWidth uint) (int, error) {
	wordSize := bits.ByteCount(dstWidth)
	remained := d.remain
	if remained == 0 {
		return 0, io.EOF
	}

	for {
		if d.offset == d.length {
			// We know that bit-pack data is encoded in chunks of 8 since the
			// bit length is divided by 8. We look for the multiple of 8
			// to maximize buffer utilization.
			chunkSizeBits := 8 * srcWidth
			numberOfChunks := bits.BitCount(len(d.buffer)) / chunkSizeBits
			numberOfBytes := bits.ByteCount(numberOfChunks * chunkSizeBits)
			// We limit the read to the number of bytes that remain to be read
			// from the underlying io.Reader, otherwise we would read beyond the
			// end of the bit-packed run.
			remainingBytes := bits.ByteCount(uint(d.remain) * srcWidth)
			if remainingBytes < numberOfBytes {
				numberOfBytes = remainingBytes
			}

			n, err := io.ReadFull(r, d.buffer[:numberOfBytes])
			if err != nil {
				return (remained - d.remain) * wordSize, err
			}

			// At this point we have the guarantee that the number of bytes read
			// is a multiple of the bit-width, the data ends at the end of a
			// byte, there will not be partial trailing words.
			d.offset = 0
			d.length = n
		}

		unpacked := bits.Pack(data, dstWidth, d.buffer[d.offset:d.length], srcWidth)
		unpackedBits := uint(unpacked) * srcWidth

		if (unpackedBits % 8) == 0 { // on a byte boundary?
			d.offset += bits.ByteCount(unpackedBits)
		} else { // move remaining bits to the beginning of the buffer
			bitShift := uint(unpackedBits)
			bitCount := bits.BitCount(d.length) - (bits.BitCount(d.offset) + unpackedBits)
			tmp := d.buffer                  // copy
			d.buffer = [bitBufferSize]byte{} // clear
			d.offset = 0
			d.length = bits.Copy(d.buffer[:], tmp[:], bitShift, bitCount)
			d.length = bits.ByteCount(uint(d.length))
		}

		d.remain -= unpacked
		data = data[bits.ByteCount(uint(unpacked)*dstWidth):]

		if d.remain == 0 || len(data) == 0 {
			return (remained - d.remain) * wordSize, nil
		}
	}
}

type runLengthDecoder struct {
	count uint32
	value [12]byte
}

func (d *runLengthDecoder) String() string { return "RUN_LENGTH" }

func (d *runLengthDecoder) decode(r io.Reader, data []byte, dstWidth, srcWidth uint) (int, error) {
	if d.count == 0 {
		return 0, io.EOF
	}

	wordSize := bits.ByteCount(dstWidth)
	count := len(data) / wordSize
	data = data[:count*wordSize]

	if n := wordSize * int(d.count); n < len(data) {
		data, count = data[:n], int(d.count)
	}

	bits.Fill(data, d.value[:wordSize])

	d.count -= uint32(count)
	return len(data), nil
}

// levelDecoder is the counter part of levelEncoder, which is used to encode
// repetition and definition levels in data page v2.
type levelDecoder struct{ *Decoder }

func newLevelDecoderSize(r io.Reader, bufferSize int) levelDecoder {
	d := levelDecoder{NewDecoderSize(r, bufferSize)}
	d.data.N, d.init = math.MaxInt64, true
	return d
}

func (d levelDecoder) Reset(r io.Reader) {
	d.Decoder.Reset(r)
	d.data.N, d.init = math.MaxInt64, true
}
