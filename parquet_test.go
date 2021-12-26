package parquet_test

import (
	"bytes"
	"fmt"
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

func forEachColumnChunk(col *parquet.Column, do func(*parquet.Column, *parquet.ColumnChunks) error) error {
	return forEachLeafColumn(col, func(leaf *parquet.Column) error {
		chunks := leaf.Chunks()

		for chunks.Next() {
			if err := do(leaf, chunks); err != nil {
				return err
			}
		}

		return nil
	})
}

func forEachColumnPage(col *parquet.Column, do func(*parquet.Column, *parquet.DataPageReader) error) error {
	return forEachColumnChunk(col, func(leaf *parquet.Column, chunks *parquet.ColumnChunks) error {
		const bufferSize = 1024
		pages := chunks.Pages()
		dictionary := (parquet.Dictionary)(nil)

		for pages.Next() {
			switch header := pages.PageHeader().(type) {
			case parquet.DictionaryPageHeader:
				decoder := header.Encoding().NewDecoder(pages.PageData())
				dictionary = leaf.Type().NewDictionary(0)

				if err := dictionary.ReadFrom(decoder); err != nil {
					return err
				}

			case parquet.DataPageHeader:
				var pageReader parquet.PageReader
				var pageData = header.Encoding().NewDecoder(pages.PageData())

				if dictionary != nil {
					pageReader = parquet.NewIndexedPageReader(pageData, bufferSize, dictionary)
				} else {
					pageReader = leaf.Type().NewPageReader(pageData, bufferSize)
				}

				dataPageReader := parquet.NewDataPageReader(
					header.RepetitionLevelEncoding().NewDecoder(pages.RepetitionLevels()),
					header.DefinitionLevelEncoding().NewDecoder(pages.DefinitionLevels()),
					header.NumValues(),
					pageReader,
					leaf.MaxRepetitionLevel(),
					leaf.MaxDefinitionLevel(),
					leaf.Index(),
					bufferSize,
				)

				if err := do(leaf, dataPageReader); err != nil {
					return err
				}

			default:
				return fmt.Errorf("unsupported page header type: %#v", header)
			}

			if err := pages.Err(); err != nil {
				return err
			}
		}
		return nil
	})
}

func forEachColumnValue(col *parquet.Column, do func(*parquet.Column, parquet.Value) error) error {
	return forEachColumnPage(col, func(leaf *parquet.Column, page *parquet.DataPageReader) error {
		for {
			v, err := page.ReadValue()
			if err != nil {
				if err != io.EOF {
					return err
				}
				break
			}
			if err := do(leaf, v); err != nil {
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
	schema := parquet.SchemaOf(rows[0])
	writer := parquet.NewWriter(w, schema, options...)

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
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
