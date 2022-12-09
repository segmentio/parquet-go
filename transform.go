package parquet

// TransformRowReader constructs a RowReader which applies the given transform
// to each row rad from reader.
//
// The transformation function writes the transformed src row to dst, returning
// the number of rows it has written. It is possible for a single row to be
// transformed to zero or more rows; transforming to zero rows is similar to
// applying a filter since the row will be skipped. If the dst buffer is not
// large enough to contain the transformation, the function must return the
// sentinel error parquet.ErrShortBuffer.
func TransformRowReader(reader RowReader, transform func(dst []Row, src Row, rowIndex int64) (int, error)) RowReader {
	return &transformRowReader{reader: reader, transform: transform}
}

type transformRowReader struct {
	reader    RowReader
	transform func([]Row, Row, int64) (int, error)
	input     transformRowBuffer
	output    transformRowBuffer
	rowIndex  int64
}

func (t *transformRowReader) ReadRows(rows []Row) (n int, err error) {
	if t.input.cap() == 0 {
		t.input.init(len(rows))
	}

	// Ensure that the transform function will always be called with empty rows,
	// whether we are writing directly to the rows buffer or using the local
	// output buffer.
	for i, row := range rows {
		rows[i] = row[:0]
	}

readRows:
	for {
		if t.output.len() > 0 {
			for _, row := range t.output.rows() {
				rows[n] = append(rows[n], row...)
				n++
				t.output.discard()
			}
		}

		for {
			if n == len(rows) {
				return n, nil
			}

			if t.input.len() == 0 {
				break
			}

			tn, err := t.transform(rows[n:], t.input.rows()[0], t.rowIndex)
			switch err {
			case nil:
				// The transform may have produced zero rows but that's OK.
				// Transforms can be used as a filtering mechanism as well even
				// if it is not their primary intent.
				n += tn
				t.input.discard()
				t.rowIndex++

			case ErrShortBuffer:
				if n > 0 {
					// There is no more space in the rows slice to transform the
					// next row but we already have results in the output so we
					// can simply return these rows and let the caller invoke us
					// again.
					return n, nil
				}
				for {
					// The rows slice is too small to contain a single row
					// transformation, we need to use the intermediary output
					// buffer to temporarily hold the results before we move
					// forward.
					if t.output.cap() == 0 {
						t.output.init(2 * (len(rows) - n))
					} else {
						t.output.init(2 * t.output.cap())
					}
					tn, err := t.transform(t.output.buffer, t.input.rows()[0], t.rowIndex)
					if err == nil {
						t.output.reset(tn)
						continue readRows
					} else if err != ErrShortBuffer {
						return n, err
					}
				}

			default:
				return n, err
			}
		}

		rn, err := t.reader.ReadRows(t.input.buffer)
		if err != nil && rn == 0 {
			return n, err
		}
		t.input.reset(rn)
	}
}

type transformRowBuffer struct {
	buffer []Row
	offset int32
	length int32
}

func (b *transformRowBuffer) init(n int) {
	b.buffer = makeRows(n)
	b.offset = 0
	b.length = 0
}

func (b *transformRowBuffer) discard() {
	row := b.buffer[b.offset]
	clearValues(row)
	b.buffer[b.offset] = row[:0]

	if b.offset++; b.offset == b.length {
		b.reset(0)
	}
}

func (b *transformRowBuffer) reset(n int) {
	b.offset = 0
	b.length = int32(n)
}

func (b *transformRowBuffer) rows() []Row {
	return b.buffer[b.offset:b.length]
}

func (b *transformRowBuffer) cap() int {
	return len(b.buffer)
}

func (b *transformRowBuffer) len() int {
	return int(b.length - b.offset)
}

// TransformRowWriter constructs a RowWriter which applies the given transform
// to each row writter to writer.
//
// The transformation function writes the transformed src row to dst, returning
// the number of rows it has written. It is possible for a single row to be
// transformed to zero or more rows; transforming to zero rows is similar to
// applying a filter since the row will be skipped. If the dst buffer is not
// large enough to contain the transformation, the function must return the
// sentinel error parquet.ErrShortBuffer.
func TransformRowWriter(writer RowWriter, transform func(dst []Row, src Row, rowIndex int64) (int, error)) RowWriter {
	return &transformRowWriter{writer: writer, transform: transform}
}

type transformRowWriter struct {
	writer    RowWriter
	transform func([]Row, Row, int64) (int, error)
	rows      []Row
	length    int
	rowIndex  int64
}

func (t *transformRowWriter) WriteRows(rows []Row) (n int, err error) {
	if len(t.rows) == 0 {
		t.rows = makeRows(len(rows))
	}

	for n < len(rows) {
		tn, err := t.transform(t.rows[t.length:], rows[n], t.rowIndex)

		switch err {
		case nil:
			if t.length += tn; t.length == len(t.rows) {
				err = t.flushRows()
			}

		case ErrShortBuffer:
			if t.length == 0 {
				t.rows = makeRows(2 * len(t.rows))
				continue
			} else {
				err = t.flushRows()
			}
		}

		if err != nil {
			return n, err
		}

		t.rowIndex++
		n++
	}

	return n, t.flushRows()
}

func (t *transformRowWriter) flushRows() error {
	defer func() {
		clearRows(t.rows[:t.length])
		t.length = 0
	}()
	_, err := t.writer.WriteRows(t.rows[:t.length])
	return err
}
