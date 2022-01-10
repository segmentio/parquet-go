package parquet_test

import (
	"bytes"
	"io"
	"reflect"

	"github.com/segmentio/parquet"
)

type Contact struct {
	Name        string `parquet:"name"`
	PhoneNumber string `parquet:"phoneNumber,optional,snappy"`
}

type AddressBook struct {
	Owner             string    `parquet:"owner,zstd"`
	OwnerPhoneNumbers []string  `parquet:"ownerPhoneNumbers,gzip"`
	Contacts          []Contact `parquet:"contacts"`
}

func forEachLeafColumn(col *parquet.Column, do func(*parquet.Column) error) error {
	children := col.Columns()

	if len(children) == 0 {
		return do(col)
	}

	for _, child := range children {
		if err := forEachLeafColumn(child, do); err != nil {
			return err
		}
	}

	return nil
}

func forEachColumnPage(col *parquet.Column, do func(*parquet.Column, parquet.Page) error) error {
	return forEachLeafColumn(col, func(leaf *parquet.Column) error {
		pages := leaf.Pages()
		for {
			p, err := pages.ReadPage()
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				return err
			}
			if err := do(leaf, p); err != nil {
				return err
			}
		}
	})
}

func forEachColumnValue(col *parquet.Column, do func(*parquet.Column, parquet.Value) error) error {
	return forEachColumnPage(col, func(leaf *parquet.Column, page parquet.Page) error {
		buffer := [1]parquet.Value{}
		values := page.Values()
		for {
			n, err := values.ReadValues(buffer[:])
			if n > 0 {
				if err := do(leaf, buffer[0]); err != nil {
					return err
				}
			}
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				return err
			}
		}
	})
}

func forEachColumnChunk(file *parquet.File, do func(*parquet.Column, parquet.ColumnChunk) error) error {
	return forEachLeafColumn(file.Root(), func(leaf *parquet.Column) error {
		for i, n := 0, file.NumRowGroups(); i < n; i++ {
			if err := do(leaf, file.RowGroup(i).Column(int(leaf.Index()))); err != nil {
				return err
			}
		}
		return nil
	})
}

func createParquetFile(rows rows, options ...parquet.WriterOption) (*parquet.File, error) {
	buffer := new(bytes.Buffer)

	if err := writeParquetFile(buffer, rows, options...); err != nil {
		return nil, err
	}

	reader := bytes.NewReader(buffer.Bytes())
	return parquet.OpenFile(reader, reader.Size())
}

func writeParquetFile(w io.Writer, rows rows, options ...parquet.WriterOption) error {
	writer := parquet.NewWriter(w, options...)

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return writer.Close()
}

func writeParquetFileWithBuffer(w io.Writer, rows rows, options ...parquet.WriterOption) error {
	buffer := parquet.NewBuffer()
	for _, row := range rows {
		if err := buffer.Write(row); err != nil {
			return err
		}
	}

	writer := parquet.NewWriter(w, options...)
	if _, err := parquet.CopyRows(writer, buffer.Rows()); err != nil {
		return err
	}
	return writer.Close()
}

type rows []interface{}

func makeRows(any interface{}) rows {
	if v, ok := any.([]interface{}); ok {
		return rows(v)
	}
	value := reflect.ValueOf(any)
	slice := make([]interface{}, value.Len())
	for i := range slice {
		slice[i] = value.Index(i).Interface()
	}
	return rows(slice)
}
