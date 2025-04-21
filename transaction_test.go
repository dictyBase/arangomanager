package arangomanager

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTransactionHandlerMethods tests the methods of TransactionHandler struct
func TestTransactionHandlerMethods(t *testing.T) {
	// Setup test environment with helper
	db, coll, cleanup := setupTestTx(t)
	defer cleanup()
	defer teardown(t, coll)

	// Begin a transaction using helper with both read and write permissions
	tx, err := db.BeginTransaction(context.Background(), &TransactionOptions{
		ReadCollections:  []string{coll.Name()},
		WriteCollections: []string{coll.Name()},
	})
	if err != nil {
		t.Fatalf("failed to begin transaction: %s", err)
	}

	t.Run("Context", func(t *testing.T) {
		assert := require.New(t)
		ctx := tx.Context()
		assert.NotNil(ctx)
	})

	t.Run("ID", func(t *testing.T) {
		assert := require.New(t)
		id := tx.ID()
		assert.NotEmpty(id)
		assert.Equal(tx.id, id)
	})

	t.Run("Status", func(t *testing.T) {
		assert := require.New(t)
		status, err := tx.Status()
		assert.NoError(err)
		assert.NotNil(status)
	})

	// Abort the transaction
	if err := tx.Abort(); err != nil {
		t.Fatalf("failed to abort transaction: %s", err)
	}
}

// TestTransactionLifecycle tests the lifecycle methods (Commit and Abort)
func TestTransactionLifecycle(t *testing.T) {
	// Setup test environment with helper
	db, coll, cleanup := setupTestTx(t)
	defer cleanup()
	defer teardown(t, coll)

	t.Run("Commit", func(t *testing.T) {
		assert := require.New(t)
		// Begin a transaction with helper
		tx := beginTestTransaction(TxParams{
			T:        t,
			DB:       db,
			Coll:     coll,
			ReadOnly: false,
		})
		assert.NotNil(tx)
		assertTxCanceled(t, tx, false)

		// Commit the transaction
		err := tx.Commit()
		assert.NoError(err)
		assertTxCanceled(t, tx, true)

		// Try to commit again, should fail
		err = tx.Commit()
		assert.Error(err)
		assert.Contains(err.Error(), "cannot commit a canceled transaction")
	})

	t.Run("Abort", func(t *testing.T) {
		assert := require.New(t)
		// Begin a transaction with helper
		tx := beginTestTransaction(TxParams{
			T:        t,
			DB:       db,
			Coll:     coll,
			ReadOnly: false,
		})
		assert.NotNil(tx)
		assertTxCanceled(t, tx, false)

		// Abort the transaction
		err := tx.Abort()
		assert.NoError(err)
		assertTxCanceled(t, tx, true)

		// Try to abort again, should fail
		err = tx.Abort()
		assert.Error(err)
		assert.Contains(err.Error(), "transaction already canceled")
	})
}

// TestTransactionDo tests the Do method
func TestTransactionDo(t *testing.T) {
	// Setup test environment with helper
	db, coll, cleanup := setupTestTx(t)
	defer cleanup()
	defer teardown(t, coll)

	t.Run("DoSuccessful", func(t *testing.T) {
		assert := require.New(t)
		// Begin a transaction with helper
		tx := beginTestTransaction(TxParams{
			T:        t,
			DB:       db,
			Coll:     coll,
			ReadOnly: false,
		})

		// Insert a document using helper
		insertTestDocument(DocParams{
			T:         t,
			TX:        tx,
			Coll:      coll,
			FirstName: "TestUser",
			LastName:  "DoMethod",
		})

		// Commit the transaction
		err := tx.Commit()
		assert.NoError(err)

		// Verify the document was inserted
		assertDocumentExists(DocExistsParams{
			T:           t,
			DB:          db,
			Coll:        coll,
			FirstName:   "TestUser",
			LastName:    "DoMethod",
			ShouldExist: true,
		})
	})

	t.Run("DoWithInvalidQuery", func(t *testing.T) {
		assert := require.New(t)
		// Begin a transaction with helper
		tx := beginTestTransaction(TxParams{
			T:        t,
			DB:       db,
			Coll:     coll,
			ReadOnly: false,
		})

		// Try to execute an invalid query
		err := tx.Do("INVALID QUERY", nil)
		assert.Error(err)

		// Abort the transaction
		err = tx.Abort()
		assert.NoError(err)
	})
}

// TestTransactionDoRun tests the DoRun method
func TestTransactionDoRun(t *testing.T) {
	// Setup test environment with helper
	db, coll, cleanup := setupTestTx(t)
	defer cleanup()
	defer teardown(t, coll)

	t.Run("DoRunSuccessful", func(t *testing.T) {
		assert := require.New(t)
		// Begin a transaction with helper
		tx := beginTestTransaction(TxParams{
			T:        t,
			DB:       db,
			Coll:     coll,
			ReadOnly: true,
		})

		// Query using DoRun method
		query := fmt.Sprintf(
			"FOR d IN %s FILTER d.gender == @gender RETURN d",
			coll.Name(),
		)
		bindVars := map[string]interface{}{"gender": "male"}

		result, err := tx.DoRun(query, bindVars)
		assert.NoError(err)
		assert.NotNil(result)
		assert.False(result.IsEmpty())

		// Abort the transaction
		err = tx.Abort()
		assert.NoError(err)
	})

	t.Run("DoRunWithInvalidQuery", func(t *testing.T) {
		assert := require.New(t)
		// Begin a transaction with helper
		tx := beginTestTransaction(TxParams{
			T:        t,
			DB:       db,
			Coll:     coll,
			ReadOnly: true,
		})

		// Try to execute an invalid query
		result, err := tx.DoRun("INVALID QUERY", nil)
		assert.Error(err)
		assert.True(result.IsEmpty())

		// Abort the transaction
		err = tx.Abort()
		assert.NoError(err)
	})
}

// TestTransactionIsolation tests the transaction isolation
func TestTransactionIsolation(t *testing.T) {
	assert := require.New(t)
	// Setup test environment with helper
	db, coll, cleanup := setupTestTx(t)
	defer cleanup()
	defer teardown(t, coll)

	// Begin a transaction with helper
	tx := beginTestTransaction(TxParams{
		T:        t,
		DB:       db,
		Coll:     coll,
		ReadOnly: false,
	})

	// Insert a document in the transaction
	insertTestDocument(DocParams{
		T:         t,
		TX:        tx,
		Coll:      coll,
		FirstName: "Isolation",
		LastName:  "Test",
	})

	// Check document not visible outside transaction
	assertDocumentExists(DocExistsParams{
		T:           t,
		DB:          db,
		Coll:        coll,
		FirstName:   "Isolation",
		LastName:    "Test",
		ShouldExist: false,
	})

	// Commit the transaction
	err := tx.Commit()
	assert.NoError(err)
	assertTxCanceled(t, tx, true)

	// Check document is now visible
	assertDocumentExists(DocExistsParams{
		T:           t,
		DB:          db,
		Coll:        coll,
		FirstName:   "Isolation",
		LastName:    "Test",
		ShouldExist: true,
	})
}
