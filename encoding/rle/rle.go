// Package rle implements the hybrid RLE/Bit-Packed encoding employed in
// repetition and definition levels, dictionary indexed data pages, and
// boolean values in the PLAIN encoding.
//
// https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
package rle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
)

type Encoding struct {
	encoding.NotSupported
	BitWidth int
}

func (e *Encoding) Encoding() format.Encoding {
	return format.RLE
}

func (e *Encoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewDecoder(r)
}

func (e *Encoding) String() string {
	return "RLE"
}

func (e *Encoding) EncodeBoolean(dst []byte, src []bool) ([]byte, error) {
	// In the case of encoding a boolean values, the 4 bytes length of the
	// output is expected by the parquet format. We add the bytes as placeholder
	// before appending the encoded data.
	dst = append(dst[:0], 0, 0, 0, 0)
	dst, err := encodeInt8(dst, bits.BytesToInt8(bits.BoolToBytes(src)), 1)
	binary.LittleEndian.PutUint32(dst, uint32(len(dst))-4)
	return dst, e.wrap(err)
}

func (e *Encoding) EncodeInt8(dst []byte, src []int8) ([]byte, error) {
	dst, err := encodeInt8(dst[:0], src, uint(e.BitWidth))
	return dst, e.wrap(err)
}

func (e *Encoding) EncodeInt32(dst []byte, src []int32) ([]byte, error) {
	dst, err := encodeInt32(dst[:0], src, uint(e.BitWidth))
	return dst, e.wrap(err)
}

func (e *Encoding) DecodeBoolean(dst []bool, src []byte) ([]bool, error) {
	if len(src) == 4 {
		return dst[:0], nil
	}
	if len(src) < 4 {
		return dst[:0], fmt.Errorf("input shorter than 4 bytes: %w", io.ErrUnexpectedEOF)
	}
	n := int(binary.LittleEndian.Uint32(src))
	src = src[4:]
	if n > len(src) {
		return dst[:0], fmt.Errorf("input shorter than length prefix: %d < %d: %w", len(src), n, io.ErrUnexpectedEOF)
	}
	buf := bits.BytesToInt8(bits.BoolToBytes(dst))
	buf, err := decodeInt8(buf[:0], src[:n], 1)
	return bits.BytesToBool(bits.Int8ToBytes(buf)), e.wrap(err)
}

func (e *Encoding) DecodeInt8(dst []int8, src []byte) ([]int8, error) {
	dst, err := decodeInt8(dst[:0], src, uint(e.BitWidth))
	return dst, e.wrap(err)
}

func (e *Encoding) DecodeInt32(dst []int32, src []byte) ([]int32, error) {
	dst, err := decodeInt32(dst[:0], src, uint(e.BitWidth))
	return dst, e.wrap(err)
}

func (e *Encoding) wrap(err error) error {
	if err != nil {
		err = encoding.Error(e, err)
	}
	return err
}

func encodeInt8(dst []byte, src []int8, bitWidth uint) ([]byte, error) {
	if bitWidth > 8 {
		return dst, errEncodeInvalidBitWidth("INT8", bitWidth)
	}
	if bitWidth == 0 {
		if !isZeroInt8(src) {
			return dst, errEncodeInvalidBitWidth("INT8", bitWidth)
		}
		return appendUvarint(dst, uint64(len(src))<<1), nil
	}

	bitMask := uint64(1<<bitWidth) - 1
	byteCount := bits.ByteCount(8 * bitWidth)

	if len(src) >= 8 {
		words := unsafe.Slice((*uint64)(unsafe.Pointer(&src[0])), len(src)/8)

		for i := 0; i < len(words); {
			j := i
			pattern := broadcast8x8(words[i] & 0xFF)

			for j < len(words) && words[j] == pattern {
				j++
			}

			if i < j {
				dst = appendUvarint(dst, uint64(8*(j-i))<<1)
				dst = append(dst, byte(pattern))
			} else {
				j++

				for j < len(words) && words[j] != broadcast8x8(words[j-1]) {
					j++
				}

				dst = appendUvarint(dst, uint64(j-i)<<1|1)

				for _, word := range words[i:j] {
					word = (word & bitMask) |
						(((word >> 8) & bitMask) << (1 * bitWidth)) |
						(((word >> 16) & bitMask) << (2 * bitWidth)) |
						(((word >> 24) & bitMask) << (3 * bitWidth)) |
						(((word >> 32) & bitMask) << (4 * bitWidth)) |
						(((word >> 40) & bitMask) << (5 * bitWidth)) |
						(((word >> 48) & bitMask) << (6 * bitWidth)) |
						(((word >> 56) & bitMask) << (7 * bitWidth))
					bits := [8]byte{}
					binary.LittleEndian.PutUint64(bits[:], word)
					dst = append(dst, bits[:byteCount]...)
				}
			}

			i = j
		}
	}

	for i := (len(src) / 8) * 8; i < len(src); {
		j := i + 1

		for j < len(src) && src[i] == src[j] {
			j++
		}

		dst = appendUvarint(dst, uint64(j-i)<<1)
		dst = append(dst, byte(src[i]))
		i = j
	}

	return dst, nil
}

func encodeInt32(dst []byte, src []int32, bitWidth uint) ([]byte, error) {
	if bitWidth > 32 {
		return dst, errEncodeInvalidBitWidth("INT32", bitWidth)
	}
	if bitWidth == 0 {
		if !isZeroInt32(src) {
			return dst, errEncodeInvalidBitWidth("INT32", bitWidth)
		}
		return appendUvarint(dst, uint64(len(src))<<1), nil
	}

	bitMask := uint32(1<<bitWidth) - 1
	byteCount := bits.ByteCount(8 * bitWidth)

	if len(src) >= 8 {
		words := unsafe.Slice((*[8]int32)(unsafe.Pointer(&src[0])), len(src)/8)

		for i := 0; i < len(words); {
			j := i
			pattern := broadcast32x8(words[i][0])

			for j < len(words) && words[j] == pattern {
				j++
			}

			if i < j {
				dst = appendUvarint(dst, uint64(8*(j-i))<<1)
				dst = appendInt32(dst, pattern[0], bitWidth)
			} else {
				j++

				for j < len(words) && words[j] != broadcast32x8(words[j-1][0]) {
					j++
				}

				dst = appendUvarint(dst, uint64(j-i)<<1|1)

				for _, word := range words[i:j] {
					bits := [9]uint32{}
					bitOffset := uint(0)

					for _, value := range word {
						i := bitOffset / 32
						j := bitOffset % 32
						bits[i+0] |= (uint32(value) & bitMask) << j
						bits[i+1] |= (uint32(value) >> (32 - j))
						bitOffset += bitWidth
					}

					b := unsafe.Slice((*byte)(unsafe.Pointer(&bits[0])), byteCount)
					dst = append(dst, b...)
				}
			}

			i = j
		}
	}

	for i := (len(src) / 8) * 8; i < len(src); {
		j := i + 1

		for j < len(src) && src[i] == src[j] {
			j++
		}

		dst = appendUvarint(dst, uint64(j-i)<<1)
		dst = appendInt32(dst, src[i], bitWidth)
		i = j
	}

	return dst, nil
}

func decodeInt8(dst []int8, src []byte, bitWidth uint) ([]int8, error) {
	if bitWidth > 8 {
		return dst, errDecodeInvalidBitWidth("INT8", bitWidth)
	}

	bitMask := uint64(1<<bitWidth) - 1
	byteCount := bits.ByteCount(8 * bitWidth)

	for i := 0; i < len(src); {
		u, n := binary.Uvarint(src[i:])
		i += n

		count, bitpack := uint(u>>1), (u&1) != 0
		if !bitpack {
			if bitWidth != 0 && (i+1) > len(src) {
				return dst, fmt.Errorf("decoding run-length block of %d values: %w", count, io.ErrUnexpectedEOF)
			}

			word := int8(0)
			if bitWidth != 0 {
				word = int8(src[i])
				i++
			}

			for count > 0 {
				dst = append(dst, word)
				count--
			}
		} else {
			for n := uint(0); n < count; n++ {
				j := i + byteCount

				if j > len(src) {
					return dst, fmt.Errorf("decoding bit-packed block of %d values: %w", 8*count, io.ErrUnexpectedEOF)
				}

				bits := [8]byte{}
				copy(bits[:], src[i:j])
				word := binary.LittleEndian.Uint64(bits[:])

				dst = append(dst,
					int8((word>>(0*bitWidth))&bitMask),
					int8((word>>(1*bitWidth))&bitMask),
					int8((word>>(2*bitWidth))&bitMask),
					int8((word>>(3*bitWidth))&bitMask),
					int8((word>>(4*bitWidth))&bitMask),
					int8((word>>(5*bitWidth))&bitMask),
					int8((word>>(6*bitWidth))&bitMask),
					int8((word>>(7*bitWidth))&bitMask),
				)

				i = j
			}
		}
	}

	return dst, nil
}

func decodeInt32(dst []int32, src []byte, bitWidth uint) ([]int32, error) {
	if bitWidth > 32 {
		return dst, errDecodeInvalidBitWidth("INT32", bitWidth)
	}

	bitMask := uint64(1<<bitWidth) - 1
	byteCount1 := bits.ByteCount(1 * bitWidth)
	byteCount8 := bits.ByteCount(8 * bitWidth)

	for i := 0; i < len(src); {
		u, n := binary.Uvarint(src[i:])
		i += n

		count, bitpack := uint(u>>1), (u&1) != 0
		if !bitpack {
			j := i + byteCount1

			if j > len(src) {
				return dst, fmt.Errorf("decoding run-length block of %d values: %w", count, io.ErrUnexpectedEOF)
			}

			bits := [4]byte{}
			copy(bits[:], src[i:j])

			word := binary.LittleEndian.Uint32(bits[:])
			i = j

			for count > 0 {
				dst = append(dst, int32(word))
				count--
			}
		} else {
			for n := uint(0); n < count; n++ {
				j := i + byteCount8

				if j > len(src) {
					return dst, fmt.Errorf("decoding bit-packed block of %d values: %w", 8*count, io.ErrUnexpectedEOF)
				}

				value := uint64(0)
				bitOffset := uint(0)

				for _, b := range src[i:j] {
					value |= uint64(b) << bitOffset

					for bitOffset += 8; bitOffset >= bitWidth; {
						dst = append(dst, int32(value&bitMask))
						value >>= bitWidth
						bitOffset -= bitWidth
					}
				}

				i = j
			}
		}
	}

	return dst, nil
}

func errEncodeInvalidBitWidth(typ string, bitWidth uint) error {
	return errInvalidBitWidth("encode", typ, bitWidth)
}

func errDecodeInvalidBitWidth(typ string, bitWidth uint) error {
	return errInvalidBitWidth("decode", typ, bitWidth)
}

func errInvalidBitWidth(op, typ string, bitWidth uint) error {
	return fmt.Errorf("cannot %s %s with invalid bit-width=%d", op, typ, bitWidth)
}

func appendUvarint(dst []byte, u uint64) []byte {
	var b [binary.MaxVarintLen64]byte
	var n = binary.PutUvarint(b[:], u)
	return append(dst, b[:n]...)
}

func appendInt32(dst []byte, v int32, bitWidth uint) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(v))
	return append(dst, b[:bits.ByteCount(bitWidth)]...)
}

func broadcast8x8(v uint64) uint64 {
	return v | v<<8 | v<<16 | v<<24 | v<<32 | v<<40 | v<<48 | v<<56
}

func broadcast32x8(v int32) [8]int32 {
	return [8]int32{v, v, v, v, v, v, v, v}
}

func isZeroInt8(data []int8) bool {
	return bytes.Count(bits.Int8ToBytes(data), []byte{0}) == len(data)
}

func isZeroInt32(data []int32) bool {
	return bytes.Count(bits.Int32ToBytes(data), []byte{0}) == (4 * len(data))
}
