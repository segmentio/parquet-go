package parquet

import "io"

// The ColumnChunk interface represents individual columns of a row group.
type ColumnChunk interface {
	// Returns the index of this column in its parent row group.
	Column() int

	// Returns a reader exposing the pages of the column.
	Pages() PageReader
}

type multiColumnChunkPageReader struct {
	pageReader      PageReader
	rowGroupColumns []ColumnChunk
}

func (r *multiColumnChunkPageReader) ReadPage() (Page, error) {
	for {
		if r.pageReader != nil {
			p, err := r.pageReader.ReadPage()
			if err == nil || err != io.EOF {
				return p, err
			}
			r.pageReader = nil
		}
		if len(r.rowGroupColumns) == 0 {
			return nil, io.EOF
		}
		r.pageReader = r.rowGroupColumns[0].Pages()
		r.rowGroupColumns = r.rowGroupColumns[1:]
	}
}
