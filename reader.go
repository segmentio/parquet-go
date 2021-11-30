package parquet

import (
	"fmt"
	"io"
)

type Reader struct {
	// WIP
}

type columnChunkReader struct {
	bufferSize         int
	typ                Type
	maxRepetitionLevel int8
	maxDefinitionLevel int8

	chunks     *ColumnChunks
	pages      *ColumnPages
	reader     *DataPageReader
	dictionary Dictionary
	numPages   int
}

func newColumnChunkReader(column *Column, config *ReaderConfig) *columnChunkReader {
	ccr := &columnChunkReader{
		bufferSize:         config.PageBufferSize,
		typ:                column.Type(),
		maxRepetitionLevel: column.MaxRepetitionLevel(),
		maxDefinitionLevel: column.MaxDefinitionLevel(),
		chunks:             column.Chunks(),
	}

	if ccr.maxRepetitionLevel > 0 || ccr.maxDefinitionLevel > 0 {
		ccr.bufferSize /= 2
	}

	return ccr
}

func (ccr *columnChunkReader) Close() error {
	ccr.chunks.close(nil)
	ccr.pages = nil
	ccr.reader = nil
	ccr.dictionary = nil
	ccr.numPages = 0
	return nil
}

func (ccr *columnChunkReader) ReadValue() (Value, error) {
readNextValue:
	if ccr.reader != nil {
		v, err := ccr.reader.ReadValue()
		switch err {
		case nil:
			return v, nil
		case io.EOF:
			ccr.reader = nil
		default:
			return Value{}, err
		}
	}

readNextPage:
	if ccr.pages != nil {
		if !ccr.pages.Next() {
			ccr.pages = nil
		} else {
			switch header := ccr.pages.PageHeader().(type) {
			case DictionaryPageHeader:
				if ccr.numPages != 0 {
					return Value{}, fmt.Errorf("the dictionary must be in the first page but one was found after reading %d pages", ccr.numPages)
				}

				ccr.dictionary = ccr.typ.NewDictionary(0)
				if err := ccr.dictionary.ReadFrom(
					header.Encoding().NewDecoder(ccr.pages.PageData()),
				); err != nil {
					return Value{}, err
				}

				ccr.numPages++
				goto readNextPage

			case DataPageHeader:
				pageReader := (PageReader)(nil)
				pageData := header.Encoding().NewDecoder(ccr.pages.PageData())

				if ccr.dictionary != nil {
					pageReader = NewIndexedPageReader(pageData, ccr.bufferSize, ccr.dictionary)
				} else {
					pageReader = ccr.typ.NewPageReader(pageData, ccr.bufferSize)
				}

				ccr.reader = NewDataPageReader(
					header.RepetitionLevelEncoding().NewDecoder(ccr.pages.RepetitionLevels()),
					header.DefinitionLevelEncoding().NewDecoder(ccr.pages.DefinitionLevels()),
					header.NumValues(),
					pageReader,
					ccr.maxRepetitionLevel,
					ccr.maxDefinitionLevel,
					ccr.bufferSize,
				)

				ccr.numPages++
				goto readNextValue

			default:
				return Value{}, fmt.Errorf("unsupported page header type: %#v", header)
			}
		}
	}

	if !ccr.chunks.Next() {
		return Value{}, io.EOF
	}

	ccr.pages = ccr.chunks.Pages()
	goto readNextPage
}
