package arangomanager

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/fatih/structs"
)

// Result is a cursor for single row of data
type Result struct {
	cursor driver.Cursor
	ctx    context.Context
	empty  bool
}

// IsEmpty checks for empty result
func (r *Result) IsEmpty() bool {
	return r.empty
}

// Read read the row of data to i interface
func (r *Result) Read(i interface{}) error {
	meta, err := r.cursor.ReadDocument(nil, i)
	if err != nil {
		return fmt.Errorf("error in reading document %s", err)
	}
	if !structs.IsStruct(i) {
		return nil
	}
	s := structs.New(i)
	if f, ok := s.FieldOk("DocumentMeta"); ok {
		if f.IsEmbedded() {
			if err := f.Set(meta); err != nil {
				return fmt.Errorf("error in assigning DocumentMeta to the structure %s", err)
			}
		}
	}
	return nil
}
