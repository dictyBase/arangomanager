package arangomanager

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/fatih/structs"
)

// Resultset is a cursor for multiple rows of result.
type Resultset struct {
	cursor driver.Cursor
	ctx    context.Context
	empty  bool
}

// IsEmpty checks for empty resultset.
func (r *Resultset) IsEmpty() bool {
	return r.empty
}

// Scan advances resultset to the next row of data.
func (r *Resultset) Scan() bool {
	if r.empty {
		return false
	}
	if r.cursor.HasMore() {
		return true
	}
	// At this point we know the cursor exists but has no more data
	// Close the cursor and ignore any errors - they'll be caught in Close() if called
	_ = r.cursor.Close()

	return false
}

// Read reads the row of data to interface i.
// Returns an error if the resultset is empty.
func (r *Resultset) Read(iface interface{}) error {
	if r.empty {
		return fmt.Errorf("cannot read from empty resultset")
	}

	meta, err := r.cursor.ReadDocument(r.ctx, iface)
	if err != nil {
		return fmt.Errorf("error in reading document %s", err)
	}
	if !structs.IsStruct(iface) {
		return nil
	}
	s := structs.New(iface)
	if f, ok := s.FieldOk("DocumentMeta"); ok {
		if f.IsEmbedded() {
			if err := f.Set(meta); err != nil {
				return fmt.Errorf(
					"error in assigning DocumentMeta to the structure %s",
					err,
				)
			}
		}
	}

	return nil
}

// Close closes the resultset and releases resources.
// If the resultset is empty (r.empty is true), the cursor is nil
// and no closing operation is needed.
func (r *Resultset) Close() error {
	if r.empty {
		return nil
	}
	if err := r.cursor.Close(); err != nil {
		return fmt.Errorf("error in closing cursor %s", err)
	}

	return nil
}
