package main

import (
	"fmt"
	"os"
	"strings"

	color "github.com/logrusorgru/aurora/v3"
	"github.com/segmentio/cli"
	"github.com/segmentio/parquet/internal/debug"
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
