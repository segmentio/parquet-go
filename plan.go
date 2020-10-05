package parquet

// Plan describes how a RowReader should behave. It describes which columns
// should be read, and the predicates to apply. The empty value is a plan that
// considers all the columns in the file's schema a do not filter any row. Can
// be used concurrently, and by multiple RowReaders.
type Plan struct {
	s *Schema
}

var DefaultPlan = &Plan{}

// schema returns the Schema needed by the Plan.
//
// It is used by RowReader to build its readers. This is probably not a great
// abstraction as it will be difficult to implement predicate pushdown, but at
// least it is compatible with what we already have.
//
// Returns nil to instruct the user to use the whole file's schema.
func (p *Plan) schema() *Schema {
	if p == nil {
		return nil
	}
	return p.s
}
