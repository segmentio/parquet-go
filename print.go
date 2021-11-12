package parquet

import (
	"io"
)

func Print(w io.Writer, name string, node Node) error {
	return PrintIndent(w, name, node, "\t", "\n")
}

func PrintIndent(w io.Writer, name string, node Node, pattern, newline string) error {
	pw := &printWriter{writer: w}
	pw.WriteString("message ")

	if name == "" {
		pw.WriteString("{")
	} else {
		pw.WriteString(name)
		pw.WriteString(" {")
	}

	if node.NumChildren() > 0 {
		pi := &printIndent{
			pattern: pattern,
			newline: newline,
			repeat:  1,
		}

		pi.writeNewLine(pw)

		for _, child := range node.Children() {
			printWithIndent(pw, child, node.ChildByName(child), pi)
			pi.writeNewLine(pw)
		}
	}

	pw.WriteString("}")
	return pw.err
}

func printWithIndent(w io.StringWriter, name string, node Node, indent *printIndent) {
	indent.writeTo(w)

	switch {
	case node.Optional():
		w.WriteString("optional ")
	case node.Repeated():
		w.WriteString("repeated ")
	default:
		w.WriteString("required ")
	}

	if node.NumChildren() == 0 {
		switch node.Type().Kind() {
		case Boolean:
			w.WriteString("boolean ")
		case Int32:
			w.WriteString("int32 ")
		case Int64:
			w.WriteString("int64 ")
		case Int96:
			w.WriteString("int96 ")
		case Float:
			w.WriteString("float ")
		case Double:
			w.WriteString("double ")
		case ByteArray:
			w.WriteString("binary ")
		case FixedLenByteArray:
			w.WriteString("fixed_len_byte_array ")
		default:
			w.WriteString("<?> ")
		}

		w.WriteString(name)

		if annotation := node.Type().Annotation(); annotation != "" {
			w.WriteString(" (")
			w.WriteString(annotation)
			w.WriteString(")")
		}

		w.WriteString(";")
	} else {
		w.WriteString("group")

		if name != "" {
			w.WriteString(" ")
			w.WriteString(name)
		}

		if annotation := node.Type().Annotation(); annotation != "" {
			w.WriteString(" (")
			w.WriteString(annotation)
			w.WriteString(")")
		}

		w.WriteString(" {")
		indent.writeNewLine(w)
		indent.push()

		for _, child := range node.Children() {
			printWithIndent(w, child, node.ChildByName(child), indent)
			indent.writeNewLine(w)
		}

		indent.pop()
		indent.writeTo(w)
		w.WriteString("}")
	}
}

type printIndent struct {
	pattern string
	newline string
	repeat  int
}

func (i *printIndent) push() {
	i.repeat++
}

func (i *printIndent) pop() {
	i.repeat--
}

func (i *printIndent) writeTo(w io.StringWriter) {
	if i.pattern != "" {
		for n := i.repeat; n > 0; n-- {
			w.WriteString(i.pattern)
		}
	}
}

func (i *printIndent) writeNewLine(w io.StringWriter) {
	if i.newline != "" {
		w.WriteString(i.newline)
	}
}

type printWriter struct {
	writer io.Writer
	err    error
}

func (w *printWriter) Write(b []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.writer.Write(b)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *printWriter) WriteString(s string) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := io.WriteString(w.writer, s)
	if err != nil {
		w.err = err
	}
	return n, err
}

var (
	_ io.StringWriter = (*printWriter)(nil)
)
