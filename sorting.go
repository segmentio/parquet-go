//go:build go1.18

package parquet

import (
	"bytes"
	"io"
	"sort"
)

// SortingWriter is a type similar to GenericWriter but it ensures that rows
// are sorted according to the sorting columns configured on the writer.
//
// The writer accumulates rows in an in-memory buffer which is sorted when it
// reaches the target number of rows, then written to a temporary row group.
// When the writer is flushed or closed, the temporary row groups are merged
// into a row group in the output file, ensuring that rows remain sorted in the
// final row group.
//
// Because row groups get encoded and compressed, they hold a lot less memory
// than if all rows were retained in memory. Sorting then merging rows chunks
// also tends to be a lot more efficient than sorting all rows in memory as it
// results in better CPU cache utilization since sorting multi-megabyte arrays
// causes a lot of cache misses since the data set cannot be held in CPU caches.
type SortingWriter[T any] struct {
	rows    *RowBuffer[T]
	buffer  *bytes.Buffer
	writer  *GenericWriter[T]
	output  *GenericWriter[T]
	maxRows int64
	numRows int64
	sorting []SortingColumn
}

// NewSortingWriter constructs a new sorting writer which writes a parquet file
// where rows of each row group are ordered according to the sorting columns
// configured on the writer.
//
// The sortRowCount argument defines the target number of rows that will be
// sorted in memory before being written to temporary row groups. The greater
// this value the more memory is needed to buffer rows in memory. Choosing a
// value that is too small limits the maximum number of rows that can exist in
// the output file since the writer cannot create more than 32K temporary row
// groups to hold the sorted row chunks.
func NewSortingWriter[T any](output io.Writer, sortRowCount int64, options ...WriterOption) *SortingWriter[T] {
	config, err := NewWriterConfig(options...)
	if err != nil {
		panic(err)
	}

	// At this time the intermediary buffer where partial row groups are
	// written is held in memory. This may prove impractical if the parquet
	// file exceeds the amount of memory available to the application, we will
	// revisit when we have a need to put this buffer on a different storage
	// medium.
	buffer := bytes.NewBuffer(nil)

	return &SortingWriter[T]{
		rows: NewRowBuffer[T](&RowGroupConfig{
			Schema:         config.Schema,
			SortingColumns: config.SortingColumns,
		}),
		buffer: buffer,
		writer: NewGenericWriter[T](buffer, &WriterConfig{
			CreatedBy:            config.CreatedBy,
			ColumnPageBuffers:    config.ColumnPageBuffers,
			ColumnIndexSizeLimit: config.ColumnIndexSizeLimit,
			PageBufferSize:       config.PageBufferSize,
			WriteBufferSize:      config.WriteBufferSize,
			DataPageVersion:      config.DataPageVersion,
			Schema:               config.Schema,
			SortingColumns:       config.SortingColumns,
			Compression:          config.Compression,
		}),
		output:  NewGenericWriter[T](output, config),
		maxRows: sortRowCount,
		sorting: config.SortingColumns,
	}
}

func (w *SortingWriter[T]) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.output.Close()
}

func (w *SortingWriter[T]) Flush() error {
	if err := w.sortAndWriteBufferedRows(); err != nil {
		return err
	}

	if w.numRows == 0 {
		return nil
	}

	defer func() {
		w.buffer.Reset()
		w.writer.Reset(w.buffer)
		w.numRows = 0
	}()

	if err := w.writer.Close(); err != nil {
		return err
	}

	f, err := OpenFile(bytes.NewReader(w.buffer.Bytes()), int64(w.buffer.Len()),
		&FileConfig{
			SkipPageIndex:    true,
			SkipBloomFilters: true,
			ReadBufferSize:   defaultReadBufferSize,
		},
	)
	if err != nil {
		return err
	}

	m, err := MergeRowGroups(f.RowGroups(),
		&RowGroupConfig{
			Schema:         w.Schema(),
			SortingColumns: w.sorting,
		},
	)
	if err != nil {
		return err
	}

	rows := m.Rows()
	defer rows.Close()

	if _, err := CopyRows(w.output, rows); err != nil {
		return err
	}

	return w.output.Flush()
}

func (w *SortingWriter[T]) Reset(output io.Writer) {
	w.rows.Reset()
	w.buffer.Reset()
	w.writer.Reset(w.buffer)
	w.output.Reset(output)
	w.numRows = 0
}

func (w *SortingWriter[T]) Write(rows []T) (int, error) {
	return w.writeRows(len(rows), func(i, j int) (int, error) { return w.rows.Write(rows[i:j]) })
}

func (w *SortingWriter[T]) WriteRows(rows []Row) (int, error) {
	return w.writeRows(len(rows), func(i, j int) (int, error) { return w.rows.WriteRows(rows[i:j]) })
}

func (w *SortingWriter[T]) writeRows(numRows int, writeRows func(i, j int) (int, error)) (int, error) {
	wn := 0

	for wn < numRows {
		if w.rows.NumRows() >= w.maxRows {
			if err := w.sortAndWriteBufferedRows(); err != nil {
				return wn, err
			}
		}

		n := int(w.maxRows - w.rows.NumRows())
		n += wn
		if n > numRows {
			n = numRows
		}

		n, err := writeRows(wn, n)
		wn += n

		if err != nil {
			return wn, err
		}
	}

	return wn, nil
}

func (w *SortingWriter[T]) SetKeyValueMetadata(key, value string) {
	w.output.SetKeyValueMetadata(key, value)
}

func (w *SortingWriter[T]) Schema() *Schema {
	return w.output.Schema()
}

func (w *SortingWriter[T]) sortAndWriteBufferedRows() error {
	if w.rows.Len() == 0 {
		return nil
	}

	defer w.rows.Reset()
	sort.Sort(w.rows)

	rows := w.rows.Rows()
	defer rows.Close()

	n, err := CopyRows(w.writer, rows)
	if err != nil {
		return err
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	w.numRows += n
	return nil
}
