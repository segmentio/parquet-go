package row

import (
	"encoding/binary"
	"fmt"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/format"
)

type fileMetaData struct {
	Version          int32                  `thrift:"1,required"`
	Schema           []format.SchemaElement `thrift:"2,required"`
	NumRows          int64                  `thrift:"3,required"`
	RowGroups        []rowGroup             `thrift:"4,required"`
	KeyValueMetadata []format.KeyValue      `thrift:"5,optional"`
	CreatedBy        string                 `thrift:"6,optional"`
}

type rowGroup struct {
	MetaData            rowGroupMetaData       `thrift:"1,optional"`
	TotalByteSize       int64                  `thrift:"2,required"`
	NumRows             int64                  `thrift:"3,required"`
	SortingColumns      []format.SortingColumn `thrift:"4,optional"`
	FileOffset          int64                  `thrift:"5,optional"`
	TotalCompressedSize int64                  `thrift:"6,optional"`
	Ordinal             int16                  `thrift:"7,optional"`
}

type rowGroupMetaData struct {
	ColumnMetaData []columnMetaData `thrift:"1,optional"`
}

type columnMetaData struct {
	Type         format.Type `thrift:"1,required"`
	PathInSchema []string    `thrift:"2,required"`
	NumValues    int64       `thrift:"3,required"`
}

// # Row Header
//
//	FIELD     | SIZE
//	--------- | -----
//	size      |     4
//	length    |     4
//	flags     |     1
//	(padding) |     3
//
// ## Row Flag
//
// bits:
//   0: has null values?
//   1: has repetition levels?
//   2: has definition levels?
// 3-7: (unused)
//
// ## Null Bitmap
//
// ## Repetition and Definition Levels
//
// # Column Header
//
// FIELD | SIZE (bytes)

const (
	sizeOfRowSize    = 4
	sizeOfRowLength  = 4
	sizeOfRowFlag    = 1
	sizeOfRowPadding = 3
	sizeOfRowHeader  = sizeOfRowSize + sizeOfRowLength + sizeOfRowFlag + sizeOfRowPadding
)

const (
	offsetOfRowSize    = 0
	offsetOfRowLength  = offsetOfRowSize + sizeOfRowSize
	offsetOfRowFlag    = offsetOfRowLength + sizeOfRowLength
	offsetOfRowPadding = offsetOfRowFlag + sizeOfRowFlag
)

const (
	hasNullValues       = 1 << 0
	hasRepetitionLevels = 1 << 1
	hasDefinitionLevels = 1 << 2
)

// type header struct {
// 	size   uint32
// 	length uint32
// 	flags  uint8
// 	_      [3]uint8
// }

/*
// bits:
// 0-3: value kind
//   4: has more than one value?
//   5: has null values?
//   6: has repetition level?
//   7: has definition level?
type tag = uint8

const (
	kindMask            = 0x7
	hasMoreThanOneValue = 1 << 4
	hasNullValues       = 1 << 5
	hasRepetitionLevel  = 1 << 6
	hasDefinitionLevel  = 1 << 7
)
*/

func marshalAppend(data []byte, row parquet.Row) ([]byte, error) {
	offset := len(data)
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0) // header placeholder

	/*
		var err error
		row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
			kind := parquet.Kind(0)
			tag := byte(0)

			for _, value := range columnValues {
				if value.RepetitionLevel() != 0 {
					tag |= hasRepetitionLevel
				}
				if value.DefinitionLevel() != 0 {
					tag |= hasDefinitionLevel
				}
				if value.IsNull() {
					tag |= hasNullValues
				} else {
					kind = value.Kind()
				}
			}

			if len(columnValues) > 1 {
				tag |= hasMoreThanOneValue
			}

			tag |= byte(kind) & kindMask
			data = append(data, tag)

			if len(columnValues) > 1 {
				data = binary.AppendUvarint(data, uint64(len(columnValues)))
			}

			if (tag & hasNullValues) != 0 {
				for i := 0; i < len(columnValues); {
					v := columnValues[i]
					b := byte(0)
					for j := uint(0); j < 8 && i < len(columnValues); {
						if !v.IsNull() {
							b |= 1 << j
						}
						i++
						j++
					}
					data = append(data, b)
				}
			}

			if (tag & hasRepetitionLevel) != 0 {
				for _, value := range columnValues {
					data = append(data, byte(value.RepetitionLevel()))
				}
			}

			if (tag & hasDefinitionLevel) != 0 {
				for _, value := range columnValues {
					data = append(data, byte(value.DefinitionLevel()))
				}
			}

			switch kind {
			case parquet.Boolean:
				for i := 0; i < len(columnValues); {
					v := columnValues[i]
					b := byte(0)
					for j := uint(0); j < 8 && i < len(columnValues); {
						b |= (v.Byte() & 1) << j
						i++
						j++
					}
					data = append(data, b)
				}

			case parquet.Int32:
				for _, value := range columnValues {
					data = plain.AppendInt32(data, value.Int32())
				}

			case parquet.Int64:
				for _, value := range columnValues {
					data = plain.AppendInt64(data, value.Int64())
				}

			case parquet.Int96:
				for _, value := range columnValues {
					data = plain.AppendInt96(data, value.Int96())
				}

			case parquet.Float:
				for _, value := range columnValues {
					data = plain.AppendFloat(data, value.Float())
				}

			case parquet.Double:
				for _, value := range columnValues {
					data = plain.AppendDouble(data, value.Double())
				}

			case parquet.ByteArray:
				for _, value := range columnValues {
					data = plain.AppendByteArray(data, value.ByteArray())
				}

			case parquet.FixedLenByteArray:
				null := ([]byte)(nil)
				size := 0

				for _, value := range columnValues {
					if !value.IsNull() {
						n := len(value.ByteArray())
						if size == 0 {
							size = n
						} else if size != n {
							err = fmt.Errorf("FIXED_LEN_BYTE_ARRAY column contains values of different sizes: %d!=%d", size, n)
							return false
						}
					}
				}

				if (tag & hasNullValues) != 0 {
					null = make([]byte, 32) // stack allocated
					if size > len(null) {
						null = make([]byte, size)
					} else {
						null = null[:size]
					}
				}

				data = binary.AppendUvarint(data, uint64(size))

				for _, value := range columnValues {
					if value.IsNull() {
						data = append(data, null...)
					} else {
						data = append(data, value.ByteArray()...)
					}
				}
			}
			return true
		})
	*/

	size := len(data) - offset
	binary.LittleEndian.PutUint32(data[offset+0:], uint32(size))
	binary.LittleEndian.PutUint32(data[offset+4:], uint32(len(row)))
	return data, nil //err
}

func unmarshalAppend(row parquet.Row, data []byte) (parquet.Row, error) {
	if len(data) < sizeOfRowHeader {
		return row, fmt.Errorf("input is shorter than the minimum: %d<%d", len(data), sizeOfRowHeader)
	}

	size := binary.LittleEndian.Uint32(data[0:])
	if size > uint32(len(data)) {
		return row, fmt.Errorf("input buffer is shorter than the row size: %d<%d", len(data), size)
	}
	data = data[sizeOfRowHeader:size]
	//columnIndex := 0

	/*
		for off := 0; off < len(data); columnIndex++ {
			tag := data[off]
			off++

			numValues := 1
			if (tag & hasMoreThanOneValue) != 0 {
				u, n := binary.Uvarint(data[off:])
				if n == 0 {
					return row, fmt.Errorf("cannot unmarshal number of values at offset %d", off)
				}
				if n < 0 || u > math.MaxInt32 {
					return row, fmt.Errorf("number of values overflow at offset %d", off)
				}
				numValues = int(u)
				off += n
			}

			nullMaskOffset := off
			if (tag & hasNullValues) != 0 {
				off += (numValues + 7) / 8
			}
			repetitionLevelOffset := off
			if (tag & hasRepetitionLevel) != 0 {
				off += numValues
			}
			definitionLevelOffset := off
			if (tag & hasDefinitionLevel) != 0 {
				off += numValues
			}

			columnStart := len(row)
			kind := parquet.Kind(tag & kindMask)

			if (tag & hasNullValues) != 0 {
				nullMask := data[nullMaskOffset : nullMaskOffset+(numValues+7)/8]
				numNulls := 0
				for _, b := range nullMask {
					numNulls += bits.OnesCount8(b)
				}
				if numNulls == numValues {
					kind = -1

					for n := 0; n < numValues; n++ {
						row = append(row, parquet.NullValue())
					}
				}
			}

			switch kind {
			case parquet.Boolean:
				end := off + (numValues+7)/8
				if end > len(data) {
					return row, plain.ErrTooShort(len(data) - off)
				}
				for n := 0; n < numValues; n++ {
					i := n / 8
					j := n % 8
					b := data[off+i] & (1 << uint(j))
					row = append(row, parquet.BooleanValue(b != 0))
				}
				off = end

			case parquet.Int32:
				end := off + 4*numValues
				if end > len(data) {
					return row, plain.ErrTooShort(len(data) - off)
				}
				for _, value := range unsafecast.BytesToInt32(data[off:end]) {
					row = append(row, parquet.Int32Value(value))
				}
				off = end

			case parquet.Int64:
				end := off + 8*numValues
				if end > len(data) {
					return row, plain.ErrTooShort(len(data) - off)
				}
				for _, value := range unsafecast.BytesToInt64(data[off:end]) {
					row = append(row, parquet.Int64Value(value))
				}
				off = end

			case parquet.Int96:
				end := off + 12*numValues
				if end > len(data) {
					return row, plain.ErrTooShort(len(data) - off)
				}
				for _, value := range deprecated.BytesToInt96(data[off:end]) {
					row = append(row, parquet.Int96Value(value))
				}
				off = end

			case parquet.Float:
				end := off + 4*numValues
				if end > len(data) {
					return row, plain.ErrTooShort(len(data) - off)
				}
				for _, value := range unsafecast.BytesToFloat32(data[off:end]) {
					row = append(row, parquet.FloatValue(value))
				}
				off = end

			case parquet.Double:
				end := off + 8*numValues
				if end > len(data) {
					return row, plain.ErrTooShort(len(data) - off)
				}
				for _, value := range unsafecast.BytesToFloat64(data[off:end]) {
					row = append(row, parquet.DoubleValue(value))
				}
				off = end

			case parquet.ByteArray:
				for n := 0; n < numValues; n++ {
					if off >= (len(data) - plain.ByteArrayLengthSize) {
						return row, plain.ErrTooShort(len(data) - off)
					}
					k := plain.ByteArrayLength(data[off:])
					if k > plain.MaxByteArrayLength {
						return row, plain.ErrTooLarge(k)
					}
					off += plain.ByteArrayLengthSize
					if k > (len(data) - off) {
						return row, plain.ErrTooShort(len(data) - off)
					}
					i, j := off, off+k
					row = append(row, parquet.ByteArrayValue(data[i:j:j]))
					off += k
				}

			case parquet.FixedLenByteArray:
				u, n := binary.Uvarint(data[off:])
				if n == 0 {
					return row, fmt.Errorf("cannot unmarshal type size at offset %d", off)
				}
				if n < 0 || u > math.MaxInt32 {
					return row, fmt.Errorf("type size overflow at offset %d", off)
				}
				off += n
				size := int(u)
				end := off + size*numValues
				if end > len(data) {
					return row, plain.ErrTooShort(len(data) - off)
				}
				for n := 0; n < numValues; n++ {
					i := off + ((n + 0) * size)
					j := off + ((n + 1) * size)
					row = append(row, parquet.FixedLenByteArrayValue(data[i:j:j]))
				}
				off = end
			}

			column := row[columnStart : columnStart+numValues]

			if (tag & hasNullValues) != 0 {
				for i := range column {
					j := i / 8
					k := i % 8
					if (data[nullMaskOffset+j] & (1 << uint(k))) == 0 {
						column[i] = parquet.NullValue()
					}
				}
			}

			hasRepetitionLevel := (tag & hasRepetitionLevel) != 0
			hasDefinitionLevel := (tag & hasDefinitionLevel) != 0
			switch {
			case hasRepetitionLevel:
				repetitionLevels := data[repetitionLevelOffset : repetitionLevelOffset+numValues]
				definitionLevels := data[definitionLevelOffset : definitionLevelOffset+numValues]
				for i, v := range column {
					repetitionLevel := int(repetitionLevels[i])
					definitionLevel := int(definitionLevels[i])
					column[i] = v.Level(repetitionLevel, definitionLevel, columnIndex)
				}
			case hasDefinitionLevel:
				definitionLevels := data[definitionLevelOffset : definitionLevelOffset+numValues]
				for i, v := range column {
					column[i] = v.Level(0, int(definitionLevels[i]), columnIndex)
				}
			default:
				for i, v := range column {
					column[i] = v.Level(0, 0, columnIndex)
				}
			}
		}
	*/
	return row, nil
}
