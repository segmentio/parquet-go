package parquet

import "reflect"

// Traversal is an interface used to implement the parquet schema traversal
// algorithm.
type Traversal interface {
	// The Traverse method is called for each column index and parquet value
	// when traversing a Go value with its parquet schema.
	//
	// The repetition and definition levels of the parquet value will be set
	// according to the structure of the input Go value.
	Traverse(columnIndex int, value Value) error
}

// TraversalFunc is an implementation of the Traverse interface for regular
// Go functions and methods.
type TraversalFunc func(int, Value) error

// Traverse satisfies the Traversal interface.
func (f TraversalFunc) Traverse(columnIndex int, value Value) error {
	return f(columnIndex, value)
}

type traverseFunc func(levels levels, value reflect.Value, traversal Traversal) error

func traverseFuncOf(columnIndex int, node Node) (int, traverseFunc) {
	optional := node.Optional()
	repeated := node.Repeated()

	if optional {
		return traverseFuncOfOptional(columnIndex, node)
	}

	if logicalType := node.Type().LogicalType(); logicalType != nil {
		switch {
		case logicalType.List != nil:
			elem := node.ChildByName("list").ChildByName("element")
			return traverseFuncOf(columnIndex, Repeated(elem))
		}
	}

	if repeated {
		return traverseFuncOfRepeated(columnIndex, node)
	}

	return traverseFuncOfRequired(columnIndex, node)
}

func traverseFuncOfOptional(columnIndex int, node Node) (int, traverseFunc) {
	columnIndex, traverse := traverseFuncOf(columnIndex, Required(node))
	return columnIndex, func(levels levels, value reflect.Value, traversal Traversal) error {
		if value.IsValid() {
			if value.IsZero() {
				value = reflect.Value{}
			} else {
				if value.Kind() == reflect.Ptr {
					value = value.Elem()
				}
				levels.definitionLevel++
			}
		}
		return traverse(levels, value, traversal)
	}
}

func traverseFuncOfRepeated(columnIndex int, node Node) (int, traverseFunc) {
	columnIndex, traverse := traverseFuncOf(columnIndex, Required(node))
	return columnIndex, func(levels levels, value reflect.Value, traversal Traversal) error {
		var numValues int
		var err error

		if value.IsValid() {
			numValues = value.Len()
			levels.repetitionDepth++
			if !value.IsNil() {
				levels.definitionLevel++
			}
		}

		if numValues == 0 {
			err = traverse(levels, reflect.Value{}, traversal)
		} else {
			for i := 0; i < numValues && err == nil; i++ {
				err = traverse(levels, value.Index(i), traversal)
				levels.repetitionLevel = levels.repetitionDepth
			}
		}

		return err
	}
}

func traverseFuncOfRequired(columnIndex int, node Node) (int, traverseFunc) {
	switch {
	case isLeaf(node):
		return traverseFuncOfLeaf(columnIndex, node)
	default:
		return traverseFuncOfGroup(columnIndex, node)
	}
}

func traverseFuncOfGroup(columnIndex int, node Node) (int, traverseFunc) {
	names := node.ChildNames()
	funcs := make([]traverseFunc, len(names))

	for i, name := range names {
		columnIndex, funcs[i] = traverseFuncOf(columnIndex, node.ChildByName(name))
	}

	valueByIndex := func(base reflect.Value, index int) reflect.Value {
		return node.ValueByName(base, names[index])
	}

	switch n := unwrap(node).(type) {
	case IndexedNode:
		valueByIndex = n.ValueByIndex
	}

	return columnIndex, func(levels levels, value reflect.Value, traversal Traversal) error {
		valueAt := valueByIndex

		if !value.IsValid() {
			valueAt = func(base reflect.Value, _ int) reflect.Value {
				return base
			}
		}

		for i, f := range funcs {
			if err := f(levels, valueAt(value, i), traversal); err != nil {
				return err
			}
		}

		return nil
	}
}

func traverseFuncOfLeaf(columnIndex int, node Node) (int, traverseFunc) {
	kind := node.Type().Kind()
	return columnIndex + 1, func(levels levels, value reflect.Value, traversal Traversal) error {
		var v Value

		if value.IsValid() {
			v = makeValue(kind, value)
		}

		v.repetitionLevel = levels.repetitionLevel
		v.definitionLevel = levels.definitionLevel
		return traversal.Traverse(columnIndex, v)
	}
}
