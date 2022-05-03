package parquet_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/segmentio/parquet-go"
)

func Example() {
	// parquet-go uses the same struct-tag definition style as JSON and XML
	type Contact struct {
		Name string `parquet:"name"`
		// "zstd" specifies the compression for this column
		PhoneNumber string `parquet:"phoneNumber,optional,zstd"`
	}

	type AddressBook struct {
		Owner             string    `parquet:"owner,zstd"`
		OwnerPhoneNumbers []string  `parquet:"ownerPhoneNumbers,gzip"`
		Contacts          []Contact `parquet:"contacts"`
	}

	f, _ := ioutil.TempFile("", "parquet-example-")
	writer := parquet.NewWriter(f)
	rows := []AddressBook{
		{Owner: "UserA", Contacts: []Contact{
			{Name: "Alice", PhoneNumber: "+15505551234"},
			{Name: "Bob"},
		}},
		// Add more rows here.
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}
	}
	_ = writer.Close()
	_ = f.Close()

	// Now, we can read from the file.
	rf, _ := os.Open(f.Name())
	pf := parquet.NewReader(rf)
	addrs := make([]AddressBook, 0)
	for {
		var addr AddressBook
		err := pf.Read(&addr)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		addrs = append(addrs, addr)
	}
	fmt.Println(addrs[0].Owner)
	// Output: UserA
}
