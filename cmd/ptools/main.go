package main

import (
	"fmt"
	"os"
	"strings"

	color "github.com/logrusorgru/aurora"
	"github.com/segmentio/centrifuge-traces/parquet/internal/debug"
	"github.com/segmentio/cli"
)

func main() {
	cli.Exec(cli.CommandSet{
		"cat": cli.Command(catCommand),
	})
}

func perrorf(format string, args ...interface{}) {
	if !strings.HasSuffix(format, "\n") {
		format += "\n"
	}
	_, _ = fmt.Fprintf(os.Stderr, color.Red(format).String(), args...)
}

func pdebugf(format string, args ...interface{}) {
	debug.Format(color.Gray(12, format).String(), args...)
}
