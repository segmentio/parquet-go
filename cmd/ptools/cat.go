// This program is a re-implementation of parquet-tools' cat command.
//
// Its goal is to output byte-to-byte what the original program outputs, on
// every parquet file, with some exceptions (to reduce scope):
//
// * --debug is free to output whatever on stderr. Only stdout is supposed to
//   match.
// * Other flags (--help, --json/-j, --no-color) are not supported.
// * It may support additional flags.
package main

import (
	"bufio"
	"encoding/base64"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"

	"github.com/segmentio/parquet"
	"github.com/segmentio/parquet/internal/debug"
	pthrift "github.com/segmentio/parquet/internal/gen-go/parquet"
)

type catFlags struct {
	_          struct{} `help:"Dump the content of the provided parquet file to stdout"`
	Debug      bool     `flag:"--debug" help:"Display debugging logs" default:"false"`
	CPUProfile string   `flag:"--cpu-profile" help:"Record a pprof CPU profile to the given file" default:"-"`
	MemProfile string   `flag:"--mem-profile" help:"Record a pprof memory profile to the given file" default:"-"`
}

func catCommand(flags catFlags, path string) {
	debug.Toggle(flags.Debug)

	if flags.CPUProfile != "" {
		f, err := os.Create(flags.CPUProfile)
		if err != nil {
			perrorf("could not create CPU profile: %s", err)
			return
		}
		defer func() {
			err := f.Close()
			if err != nil {
				perrorf("could not close CPU profile: %s", err)
			}
		}()
		if err := pprof.StartCPUProfile(f); err != nil {
			perrorf("could not start CPU profile: %s", err)
			return
		}
		debug.Format("started CPU profile to %s", flags.CPUProfile)
		defer pprof.StopCPUProfile()
	}

	file, err := os.Open(path)
	if err != nil {
		perrorf("Could not open file: %s", err)
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			perrorf("Could not close file: %s", err)
		}
	}()
	f, err := parquet.OpenFile(file)
	if err != nil {
		perrorf("Could parse parquet file: %s", err)
		return
	}

	printer := newPrettyPrinter(os.Stdout)
	rowReader := parquet.NewRowReader(f)

	for {
		err := rowReader.Read(printer)
		if err == parquet.EOF {
			break
		}
		if err != nil {
			perrorf("error: %s", err)
		}
	}

	if flags.MemProfile != "" {
		f, err := os.Create(flags.MemProfile)
		if err != nil {
			perrorf("could not create memory profile: %s", err)
			return
		}
		defer func() {
			err := f.Close()
			if err != nil {
				perrorf("could not close memory profile: %s", err)
			}
		}()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			perrorf("could not write memory profile: %s", err)
		} else {
			debug.Format("wrote memory profile at %s", flags.MemProfile)
		}
	}
}

type prettyPrinter struct {
	writer  *bufio.Writer
	scratch []byte
	b64     []byte
	stack   []*parquet.Schema
}

func newPrettyPrinter(w io.Writer) *prettyPrinter {
	return &prettyPrinter{
		writer:  bufio.NewWriter(w),
		scratch: make([]byte, 64),
		stack:   make([]*parquet.Schema, 32),
	}
}

func (p *prettyPrinter) stackClear() {
	p.stack = p.stack[:0]
}

func (p *prettyPrinter) stackPush(s *parquet.Schema) {
	p.stack = append(p.stack, s)
}

func (p *prettyPrinter) stackPop() {
	p.stack = p.stack[:len(p.stack)-1]
}

func (p *prettyPrinter) stackPeek() *parquet.Schema {
	return p.stack[len(p.stack)-1]
}

func (p *prettyPrinter) depth() int {
	return p.prefix()
}

func (p *prettyPrinter) prefix() int {
	prefix := 0
	if len(p.stack) == 0 {
		return prefix
	}
	parent := p.stackPeek()
	if parent != nil {
		prefix = len(parent.Path)
	}
	return prefix
}

func (p *prettyPrinter) Begin() {
	p.stackClear()
}

func (p *prettyPrinter) Primitive(s *parquet.Schema, d parquet.Decoder) error {
	valueDepth := p.depth()
	for _, c := range s.Path[p.prefix() : len(s.Path)-1] {
		p.writeDepth(valueDepth)
		p.writeString(c)
		p.writeString(":\n")
		valueDepth++
	}
	p.writeDepth(valueDepth)
	p.writeString(s.Name)
	p.writeString(" = ")

	switch s.PhysicalType {
	case pthrift.Type_INT32:
		v, err := d.Int32()
		if err != nil {
			return err
		}
		p.scratch = p.ensure(p.scratch, 20) // digits of max uint64 base 10
		b := strconv.AppendInt(p.scratch[:0], int64(v), 10)
		_, _ = p.writer.Write(b)
	case pthrift.Type_INT64:
		v, err := d.Int64()
		if err != nil {
			return err
		}
		p.scratch = p.ensure(p.scratch, 20) // digits of max uint64 base 10
		b := strconv.AppendInt(p.scratch[:0], v, 10)
		_, _ = p.writer.Write(b)
	case pthrift.Type_BYTE_ARRAY:
		var err error
		p.scratch, err = d.ByteArray(p.scratch)
		if err != nil {
			return err
		}
		if s.ConvertedType == nil {
			p.writeBase64(p.scratch)
		} else if *s.ConvertedType == pthrift.ConvertedType_UTF8 {
			_, _ = p.writer.Write(p.scratch)
		}
	default:
		panic("UNSUPPORTED")
	}
	p.writeLn()

	return nil
}

func (p *prettyPrinter) PrimitiveNil(s *parquet.Schema) error {
	return nil
}

func (p *prettyPrinter) GroupBegin(s *parquet.Schema) {
	p.writeGroupPrefix(s)
	p.stackPush(s)
}

func (p *prettyPrinter) GroupEnd(s *parquet.Schema) {
	p.stackPop()
}

func (p *prettyPrinter) RepeatedBegin(s *parquet.Schema) {
	p.writeGroupPrefix(s)
	p.stackPush(s)
}

func (p *prettyPrinter) RepeatedEnd(s *parquet.Schema) {
	p.stackPop()
}

func (p *prettyPrinter) KVBegin(s *parquet.Schema) {
	p.writeGroupPrefix(s)
	p.stackPush(s)
}

func (p *prettyPrinter) KVEnd(s *parquet.Schema) {
	p.stackPop()
}

func (p *prettyPrinter) End() {
	p.writeLn()
	_ = p.writer.Flush()
}

func (p *prettyPrinter) writeDepth(depth int) {
	for i := 0; i < depth; i++ {
		_ = p.writer.WriteByte('.')
	}
}

func (p *prettyPrinter) writeString(s string) {
	_, _ = p.writer.WriteString(s)
}

func (p *prettyPrinter) writeBase64(b []byte) {
	size := base64.StdEncoding.EncodedLen(len(b))
	p.b64 = p.ensure(p.b64, size)
	base64.StdEncoding.Encode(p.b64, b)
	_, _ = p.writer.Write(p.b64)
}

func (p *prettyPrinter) writeLn() {
	_ = p.writer.WriteByte('\n')
}

func (p *prettyPrinter) writeGroupPrefix(s *parquet.Schema) {
	v := p.depth()
	for _, c := range s.Path[p.prefix():len(s.Path)] {
		p.writeDepth(v)
		v++
		p.writeString(c)
		p.writeString(":\n")
	}
}

func (p *prettyPrinter) ensure(b []byte, size int) []byte {
	if cap(b) < size {
		return make([]byte, size)
	}
	return b[:size]
}
