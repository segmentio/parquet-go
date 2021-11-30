package rle

import (
	"fmt"
	"io"
	. "math/bits"

	"github.com/segmentio/parquet/internal/bits"
)

// Large enougth to hold 64 x int64
const bitPackBufferSize = 512

type bitPackRunDecoder struct {
	reader io.LimitedReader
	// Offset and length of values read into the buffer, and number of values
	// left to be consumed.
	offset uint
	length uint
	remain uint
	// The bit width of values read by the encoder, and capacity of the buffer,
	// which is computed to be the greatest multiple of 8 x bit-width.
	bitWidth uint
	capacity uint
	// Buffer where bits are loaded from the io.Reader; the size is large enough
	// to contain 64 int64 values, since bit-pack integers are encoded in chunks
	// of 8 values. Most of the time the source bit-width is a lot smaller than
	// 64 bits (commonly 2-3 bits), so a lot more values get loaded in each read
	// from the io.Reader.
	buffer [bitPackBufferSize]byte
}

func (d *bitPackRunDecoder) String() string { return "BIT_PACK" }

func (d *bitPackRunDecoder) reset(r io.Reader, bitWidth, numValues uint) {
	d.reader.R = r
	d.reader.N = int64(bits.ByteCount(numValues * bitWidth))
	d.offset = 0
	d.length = 0
	d.remain = 0
	d.bitWidth = bitWidth
	d.capacity = (bitPackBufferSize / (8 * bitWidth)) * 8 * bitWidth
}

func (d *bitPackRunDecoder) decode(dst []byte, dstWidth uint) (int, error) {
	dstBitCount := bits.BitCount(len(dst))

	if dstWidth < 8 || dstWidth > 64 || OnesCount(dstWidth) != 1 {
		return 0, fmt.Errorf("BIT_PACK decoder expects the output size to be a power of 8 bits but got %d bits", dstWidth)
	}

	if (dstBitCount & (dstWidth - 1)) != 0 { // (dstBitCount % dstWidth) != 0
		return 0, fmt.Errorf("BIT_PACK decoder expects the input size to be a multiple of the destination width: bit-count=%d bit-width=%d",
			dstBitCount, dstWidth)
	}

	if dstWidth < d.bitWidth {
		return 0, fmt.Errorf("BIT_PACK decoder cannot encode %d bits values to %d bits: the source width must be less or equal to the destination width",
			d.bitWidth, dstWidth)
	}

	numValues := 0
	dstSize := bits.ByteCount(dstWidth)

	for len(dst) != 0 {
		if d.remain != 0 {
			n := bits.Pack(dst, dstWidth, d.buffer[d.offset:d.length], d.bitWidth)
			bitOffset := (d.offset * 8) + (d.bitWidth * uint(n))
			index, shift := bits.IndexShift8(bitOffset)
			bits.ShiftRight(d.buffer[index:d.length], shift)
			d.offset = index
			d.remain -= uint(n)
			dst = dst[n*dstSize:]
			numValues += n
			continue
		}

		n, err := io.ReadFull(&d.reader, d.buffer[:d.capacity])
		if err != nil && n == 0 {
			return numValues, err
		}
		if ((bits.BitCount(n) / d.bitWidth) % 8) != 0 {
			return numValues, fmt.Errorf("BIT_PACK decoder expects sequences of 8 values but %d were read", bits.BitCount(n)/d.bitWidth)
		}

		d.offset = 0
		d.length = uint(n)
		d.remain = bits.BitCount(n) / d.bitWidth
	}

	return numValues, nil
}

type bitPackRunEncoder struct {
	writer   io.Writer
	bitWidth uint
	buffer   [bitPackBufferSize]byte
}

func (e *bitPackRunEncoder) reset(w io.Writer, bitWidth uint) {
	e.writer, e.bitWidth = w, bitWidth
}

func (e *bitPackRunEncoder) encode(src []byte, srcWidth uint) error {
	srcBitCount := bits.BitCount(len(src))

	if srcWidth < 8 || srcWidth > 64 || OnesCount(srcWidth) != 1 {
		return fmt.Errorf("BIT_PACK encoder expects the input size to be a power of 8 bits but got %d bits", srcWidth)
	}

	if (srcBitCount & (srcWidth - 1)) != 0 { // (srcBitCount % srcWidth) != 0
		return fmt.Errorf("BIT_PACK encoder expects the input size to be a multiple of the source width: bit-count=%d bit-width=%d", srcBitCount, srcWidth)
	}

	if ((srcBitCount / srcWidth) % 8) != 0 {
		return fmt.Errorf("BIT_PACK encoder expects sequences of 8 values but %d were written", srcBitCount/srcWidth)
	}

	if srcWidth < e.bitWidth {
		return fmt.Errorf("BIT_PACK encoder cannot encode %d bits values to %d bits: the source width must be less or equal to the destination width",
			srcWidth, e.bitWidth)
	}

	if srcWidth == e.bitWidth {
		_, err := e.writer.Write(src)
		return err
	}

	bytesPerLoop := (bitPackBufferSize / (8 * e.bitWidth)) * (8 * srcWidth)

	for len(src) > 0 {
		i := bytesPerLoop
		if i > uint(len(src)) {
			i = uint(len(src))
		}

		n := bits.Pack(e.buffer[:], e.bitWidth, src[:i], srcWidth)
		_, err := e.writer.Write(e.buffer[:bits.ByteCount(uint(n)*e.bitWidth)])
		if err != nil {
			return err
		}

		src = src[i:]
	}

	return nil
}
