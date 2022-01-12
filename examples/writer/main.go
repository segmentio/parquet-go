package main

import (
	"log"
	"os"
	"time"

	"github.com/segmentio/parquet"
)

type Measure struct {
	Name      string   `parquet:"name,delta"`
	Labels    []string `parquet:"labels,dict"`
	Timestamp int64    `parquet:"timestamp,plain"`
	Value     int64    `parquet:"value,plain"`
}

func main() {

	tmp, err := os.CreateTemp("/tmp", "*.parquet")
	if err != nil {
		log.Fatal(err)
	}

	schema := parquet.SchemaOf(new(Measure))
	log.Println(schema.String())

	config := parquet.DefaultWriterConfig()
	config.Schema = schema

	writer := parquet.NewWriter(tmp, config)

	for _, row := range makeData() {
		if err := writer.Write(row); err != nil {
			log.Printf("failed to write row: %s", err)
		}
	}

	if err := writer.Close(); err != nil {
		log.Printf("failed to close writer: %s", err)
	}

	log.Println(tmp.Name())

}

func makeData() []Measure {
	now := time.Now()

	labels := []string{
		"host:srv1",
		"region:eu",
	}

	return []Measure{
		{"requests_total", labels, now.Unix(), 1},
		{"requests_bytes", labels, now.Add(time.Second).Unix(), 128},
	}
}
