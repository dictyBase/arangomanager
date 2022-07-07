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
		return r.empty
	}
	if r.cursor.HasMore() {
		return true
	}
	r.cursor.Close()

	return false
}

// Read read the row of data to interface i.
func (r *Resultset) Read(iface interface{}) error {
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
				return fmt.Errorf("error in assigning DocumentMeta to the structure %s", err)
			}
		}
	}

	return nil
}

// Close closed the resultset.
func (r *Resultset) Close() error {
	if err := r.cursor.Close(); err != nil {
		return fmt.Errorf("error in closing cursor %s", err)
	}

	return nil
}
