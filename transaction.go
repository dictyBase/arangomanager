package arangomanager

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
)

// TransactionHandler represents a transaction with begin/commit/abort capabilities
type TransactionHandler struct {
	db       *Database
	id       driver.TransactionID
	ctx      context.Context
	canceled bool
}

// Context returns the transaction context which should be used for all operations within the transaction
func (t *TransactionHandler) Context() context.Context {
	return t.ctx
}

// ID returns the transaction ID
func (t *TransactionHandler) ID() driver.TransactionID {
	return t.id
}

// Commit commits the transaction
func (t *TransactionHandler) Commit() error {
	if t.canceled {
		return fmt.Errorf("cannot commit a canceled transaction")
	}

	if err := t.db.dbh.CommitTransaction(context.Background(), t.id, nil); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	t.canceled = true
	return nil
}

// Abort aborts the transaction
func (t *TransactionHandler) Abort() error {
	if t.canceled {
		return fmt.Errorf("transaction already canceled")
	}

	if err := t.db.dbh.AbortTransaction(context.Background(), t.id, nil); err != nil {
		return fmt.Errorf("failed to abort transaction: %w", err)
	}

	t.canceled = true
	return nil
}

// Status retrieves the current status of the transaction
func (t *TransactionHandler) Status() (driver.TransactionStatusRecord, error) {
	status, err := t.db.dbh.TransactionStatus(context.Background(), t.id)
	if err != nil {
		return driver.TransactionStatusRecord{}, fmt.Errorf(
			"failed to get transaction status: %w",
			err,
		)
	}

	return status, nil
}

// Do executes a query within the transaction.
func (t *TransactionHandler) Do(
	query string,
	bindVars map[string]interface{},
) error {
	ctx := driver.WithSilent(t.ctx)
	_, err := t.db.dbh.Query(ctx, query, bindVars)
	if err != nil {
		return fmt.Errorf("error in data modification query %w", err)
	}

	return nil
}

// DoRunTransaction executes a query within a transaction that returns a
// result.
func (t *TransactionHandler) DoRun(
	query string,
	bindVars map[string]interface{},
) (*Result, error) {
	if err := t.db.dbh.ValidateQuery(t.ctx, query); err != nil {
		return &Result{
				empty: true,
			}, fmt.Errorf(
				"error in validating the query %s",
				err,
			)
	}
	cqr, err := t.db.dbh.Query(t.ctx, query, bindVars)
	return t.db.getResult(cqr, err)
}
