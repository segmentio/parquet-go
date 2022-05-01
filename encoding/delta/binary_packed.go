package delta

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
	"github.com/segmentio/parquet-go/internal/bits"
)

const (
	blockSize     = 128
	numMiniBlocks = 4
	miniBlockSize = blockSize / numMiniBlocks
)

type BinaryPackedEncoding struct {
	encoding.NotSupported
}

func (e *BinaryPackedEncoding) Encoding() format.Encoding {
	return format.DeltaBinaryPacked
}

func (e *BinaryPackedEncoding) NewDecoder(r io.Reader) encoding.Decoder {
	return NewBinaryPackedDecoder(r)
}

func (e *BinaryPackedEncoding) NewEncoder(w io.Writer) encoding.Encoder {
	return NewBinaryPackedEncoder(w)
}

func (e *BinaryPackedEncoding) String() string {
	return "DELTA_BINARY_PACKED"
}

func (e *BinaryPackedEncoding) EncodeInt32(dst []byte, src []int32) ([]byte, error) {
	return e.encodeInt32(dst[:0], src)
}

func (e *BinaryPackedEncoding) EncodeInt64(dst []byte, src []int64) ([]byte, error) {
	return e.encodeInt64(dst[:0], src)
}

func (e *BinaryPackedEncoding) encodeInt32(dst []byte, src []int32) ([]byte, error) {
	return e.encode(dst, len(src), func(i int) int64 { return int64(src[i]) })
}

func (e *BinaryPackedEncoding) encodeInt64(dst []byte, src []int64) ([]byte, error) {
	return e.encode(dst, len(src), func(i int) int64 { return src[i] })
}

func (e *BinaryPackedEncoding) encode(dst []byte, totalValues int, valueAt func(int) int64) ([]byte, error) {
	firstValue := int64(0)
	if totalValues > 0 {
		firstValue = valueAt(0)
	}
	dst = appendBinaryPackedHeader(dst, blockSize, numMiniBlocks, totalValues, firstValue)
	if totalValues < 2 {
		return dst, nil
	}

	lastValue := firstValue
	for i := 1; i < totalValues; {
		block := make([]int64, blockSize)
		n := blockSize
		r := totalValues - i
		if n > r {
			n = r
		}
		block = block[:n]
		for j := range block {
			block[j] = valueAt(i)
			i++
		}

		for j, v := range block {
			block[j], lastValue = v-lastValue, v
		}

		minDelta := bits.MinInt64(block)
		bits.SubInt64(block, minDelta)

		miniBlock := make([]byte, blockSize*4)
		bitWidths := make([]byte, numMiniBlocks)
		bitOffset := uint(0)
		miniBlockLength := 0

		for i := range bitWidths {
			j := (i + 0) * miniBlockSize
			k := (i + 1) * miniBlockSize

			if k > len(block) {
				k = len(block)
			}

			bitWidth := uint(bits.MaxLen64(block[j:k]))
			if bitWidth != 0 {
				bitWidths[i] = byte(bitWidth)

				for _, bits := range block[j:k] {
					for b := uint(0); b < bitWidth; b++ {
						x := bitOffset / 8
						y := bitOffset % 8
						miniBlock[x] |= byte(((bits >> b) & 1) << y)
						bitOffset++
					}
				}

				miniBlockLength += (miniBlockSize * int(bitWidth)) / 8
			}

			if k == len(block) {
				break
			}
		}

		miniBlock = miniBlock[:miniBlockLength]
		dst = appendBinaryPackedBlock(dst, int64(minDelta), bitWidths)
		dst = append(dst, miniBlock...)
	}

	return dst, nil
}

func (e *BinaryPackedEncoding) DecodeInt32(dst []int32, src []byte) ([]int32, error) {
	dst, _, err := e.decodeInt32(dst[:0], src)
	return dst, err
}

func (e *BinaryPackedEncoding) DecodeInt64(dst []int64, src []byte) ([]int64, error) {
	dst, _, err := e.decodeInt64(dst[:0], src)
	return dst, err
}

func (e *BinaryPackedEncoding) decodeInt32(dst []int32, src []byte) ([]int32, []byte, error) {
	src, err := e.decode(src, func(value int64) { dst = append(dst, int32(value)) })
	return dst, src, err
}

func (e *BinaryPackedEncoding) decodeInt64(dst []int64, src []byte) ([]int64, []byte, error) {
	src, err := e.decode(src, func(value int64) { dst = append(dst, value) })
	return dst, src, err
}

func (e *BinaryPackedEncoding) decode(src []byte, observe func(int64)) ([]byte, error) {
	src, blockSize, numMiniBlocks, totalValues, firstValue, err := readBinaryPackedHeader(src)
	if err != nil {
		return src, encoding.Error(e, err)
	}
	if totalValues == 0 {
		return src, nil
	}

	observe(firstValue)
	totalValues--
	lastValue := firstValue
	numValuesInMiniBlock := blockSize / numMiniBlocks

	block := make([]int64, 128)
	if cap(block) < blockSize {
		block = make([]int64, blockSize)
	} else {
		block = block[:blockSize]
	}

	for totalValues > 0 && len(src) > 0 {
		var minDelta int64
		var bitWidths []byte
		src, minDelta, bitWidths = readBinaryPackedBlock(src, numMiniBlocks)

		blockOffset := 0
		for i := range block {
			block[i] = 0
		}

		for _, bitWidth := range bitWidths {
			if bitWidth == 0 {
				blockOffset += numValuesInMiniBlock
			} else {
				miniBlockSize := (numValuesInMiniBlock * int(bitWidth)) / 8
				miniBlockData := ([]byte)(nil)

				if len(src) < miniBlockSize {
					miniBlockSize = len(src)
				}

				miniBlockData, src = src[:miniBlockSize], src[miniBlockSize:]
				bitOffset := uint(0)

				for i := 0; i < numValuesInMiniBlock; i++ {
					delta := int64(0)

					for b := uint(0); b < uint(bitWidth); b++ {
						x := bitOffset / 8
						y := bitOffset % 8
						delta |= int64((miniBlockData[x]>>y)&1) << b
						bitOffset++
					}

					block[blockOffset] = delta
					blockOffset++
				}
			}

			if len(src) == 0 {
				break
			}
		}

		bits.AddInt64(block, minDelta)
		block[0] += lastValue
		for i := 1; i < len(block); i++ {
			block[i] += block[i-1]
		}

		values := block
		if len(values) > totalValues {
			values = values[:totalValues]
		}
		for _, v := range values {
			observe(v)
		}
		lastValue = values[len(values)-1]
		totalValues -= len(values)
	}

	if totalValues > 0 {
		return src, encoding.Errorf(e, "%d missing values: %w", totalValues, io.ErrUnexpectedEOF)
	}

	return src, nil
}

func appendBinaryPackedHeader(dst []byte, blockSize, numMiniBlocks, totalValues int, firstValue int64) []byte {
	b := [4 * binary.MaxVarintLen64]byte{}
	n := 0
	n += binary.PutUvarint(b[n:], uint64(blockSize))
	n += binary.PutUvarint(b[n:], uint64(numMiniBlocks))
	n += binary.PutUvarint(b[n:], uint64(totalValues))
	n += binary.PutVarint(b[n:], firstValue)
	return append(dst, b[:n]...)
}

func appendBinaryPackedBlock(dst []byte, minDelta int64, bitWidths []byte) []byte {
	b := [binary.MaxVarintLen64]byte{}
	n := binary.PutVarint(b[:], minDelta)
	dst = append(dst, b[:n]...)
	dst = append(dst, bitWidths...)
	return dst
}

func readBinaryPackedHeader(src []byte) (next []byte, blockSize, numMiniBlocks, totalValues int, firstValue int64, err error) {
	u := uint64(0)
	n := 0
	i := 0

	u, n = binary.Uvarint(src[i:])
	i += n
	blockSize = int(u)

	u, n = binary.Uvarint(src[i:])
	i += n
	numMiniBlocks = int(u)

	u, n = binary.Uvarint(src[i:])
	i += n
	totalValues = int(u)

	firstValue, n = binary.Varint(src[i:])
	i += n

	if numMiniBlocks == 0 {
		err = fmt.Errorf("invalid number of mini block (%d)", numMiniBlocks)
	} else if (blockSize <= 0) || (blockSize%128) != 0 {
		err = fmt.Errorf("invalid block size is not a multiple of 128 (%d)", blockSize)
	} else if miniBlockSize := blockSize / numMiniBlocks; (numMiniBlocks <= 0) || (miniBlockSize%32) != 0 {
		err = fmt.Errorf("invalid mini block size is not a multiple of 32 (%d)", miniBlockSize)
	} else if totalValues < 0 {
		err = fmt.Errorf("invalid total number of values is negative (%d)", totalValues)
	}

	return src[i:], blockSize, numMiniBlocks, totalValues, firstValue, err
}

func readBinaryPackedBlock(src []byte, numMiniBlocks int) (next []byte, minDelta int64, bitWidths []byte) {
	minDelta, n := binary.Varint(src)
	src = src[n:]
	if len(src) < numMiniBlocks {
		bitWidths, src = src, nil
	} else {
		bitWidths, src = src[:numMiniBlocks], src[numMiniBlocks:]
	}
	return src, minDelta, bitWidths
}
