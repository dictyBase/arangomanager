package arangomanager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyResultsetScan(t *testing.T) {
	// Create an empty resultset
	rs := &Resultset{empty: true}
	// Test Scan
	assert := require.New(t)
	assert.False(rs.Scan(), "Scan on empty resultset should return false")

	// Test multiple Scan calls on empty resultset
	assert.False(
		rs.Scan(),
		"Multiple Scan calls on empty resultset should return false",
	)

	// Test Close
	err := rs.Close()
	assert.NoError(err, "Close on empty resultset should not return error")
}

func TestEmptyResultsetClose(t *testing.T) {
	// Create an empty resultset
	rs := &Resultset{empty: true}

	// Test Close
	assert := require.New(t)
	err := rs.Close()
	assert.NoError(err, "Close on empty resultset should not return error")

	// Test multiple Close calls
	err = rs.Close()
	assert.NoError(
		err,
		"Multiple Close calls on empty resultset should not return error",
	)
}

func TestEmptyResultsetIsEmpty(t *testing.T) {
	// Create an empty resultset
	rs := &Resultset{empty: true}

	// Test IsEmpty
	assert := assert.New(t)
	assert.True(rs.IsEmpty(), "IsEmpty on empty resultset should return true")
}

// TestResultsetWorkflow tests the complete workflow of using a Resultset
// including handling empty cursor cases
func TestResultsetWorkflow(t *testing.T) {
	assert := assert.New(t)

	// Case 1: Empty resultset
	rs1 := &Resultset{empty: true}
	assert.True(rs1.IsEmpty(), "IsEmpty should return true for empty resultset")
	assert.False(rs1.Scan(), "Scan should return false for empty resultset")

	// Attempt to read from empty resultset (should not panic, though would error in actual use)
	var data struct{}
	err := rs1.Read(&data)
	assert.Error(err, "Read on empty resultset should return error")

	// Close empty resultset
	err = rs1.Close()
	assert.NoError(err, "Close on empty resultset should not return error")

	// Case 2: Multiple operations in sequence (complete workflow)
	// This would typically cause issues if the cursor is already closed
	rs2 := &Resultset{empty: true}
	assert.False(rs2.Scan(), "First scan should return false")
	assert.False(rs2.Scan(), "Second scan should also return false")
	assert.NoError(rs2.Close(), "First close should not error")
	assert.NoError(rs2.Close(), "Second close should not error")
}
