# ArangoManager

[![License](https://img.shields.io/badge/License-BSD%202--Clause-blue.svg)](LICENSE)   
![Continuous integration](https://github.com/dictyBase/arangomanager/workflows/Continuous%20integration/badge.svg)
[![GoDoc](https://pkg.go.dev/badge/github.com/dictyBase/arangomanager)](https://pkg.go.dev/github.com/dictyBase/arangomanager)
[![codecov](https://codecov.io/gh/dictyBase/arangomanager/branch/develop/graph/badge.svg)](https://codecov.io/gh/dictyBase/arangomanager)

A Go library providing utilities and abstractions for working with [ArangoDB](https://arangodb.com).

## Features

- Connection management and session handling
- Database operations (create, find, drop)
- Collection operations (create, find, truncate)
- Query execution with support for parameters
- Result set handling
- Index management (geo, hash, persistent, skiplist)
- Graph operations
- User management with access control

## Installation

```bash
go get github.com/dictyBase/arangomanager
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/dictyBase/arangomanager"
)

func main() {
    // Connect to ArangoDB
    connParams := &arangomanager.ConnectParams{
        User:     "root",
        Pass:     "password",
        Database: "mydb",
        Host:     "localhost",
        Port:     8529,
        Istls:    false,
    }
    
    // Create session and database connection
    session, db, err := arangomanager.NewSessionDb(connParams)
    if err != nil {
        log.Fatalf("failed to connect: %s", err)
    }
    
    // Execute a query
    query := "FOR u IN users FILTER u.name == @name RETURN u"
    bindVars := map[string]interface{}{
        "name": "John",
    }
    
    result, err := db.GetRow(query, bindVars)
    if err != nil {
        log.Fatalf("query failed: %s", err)
    }
    
    if !result.IsEmpty() {
        var user struct {
            Name  string `json:"name"`
            Email string `json:"email"`
        }
        if err := result.Read(&user); err != nil {
            log.Fatalf("failed to read result: %s", err)
        }
        fmt.Printf("User: %s, Email: %s\n", user.Name, user.Email)
    }
}
```

## Main Components

### Session

The `Session` type manages the connection to the ArangoDB server:

```go
// Create a new session
session, err := arangomanager.Connect(host, user, password, port, isTLS)

// Get a database instance
db, err := session.DB("myDatabase")

// Create a new database
err := session.CreateDB("newDatabase", nil)
```

### Database

The `Database` type provides methods for interacting with an ArangoDB database:

```go
// Execute a query returning multiple rows
rs, err := db.SearchRows(query, bindVars)

// Execute a query returning a single row
row, err := db.GetRow(query, bindVars)

// Count results from a query
count, err := db.CountWithParams(query, bindVars)

// Execute a modification query
err := db.Do(query, bindVars)

// Create a collection
coll, err := db.CreateCollection("myCollection", nil)

// Find or create a collection
coll, err := db.FindOrCreateCollection("myCollection", nil)

// Create indices
idx, created, err := db.EnsureHashIndex("myCollection", []string{"field1"}, opts)
```

### ResultSet

The `Resultset` type handles query results with multiple rows:

```go
rs, err := db.SearchRows(query, bindVars)
if err != nil {
    // handle error
}
defer rs.Close()

// Check if result is empty
if rs.IsEmpty() {
    // handle empty result
}

// Iterate through results
for rs.Scan() {
    var item MyType
    if err := rs.Read(&item); err != nil {
        // handle error
    }
    // process item
}
```

### Result

The `Result` type handles query results with a single row:

```go
res, err := db.GetRow(query, bindVars)
if err != nil {
    // handle error
}

// Check if result is empty
if res.IsEmpty() {
    // handle empty result
}

// Read data into struct
var item MyType
if err := res.Read(&item); err != nil {
    // handle error
}
```

## Testing with TestArango

The `testarango` package provides utilities for writing tests against ArangoDB
without affecting your production databases.

### Features

- Creates isolated, disposable test databases for your tests
- Automatically cleans up after tests complete
- Configurable via environment variables or direct parameter passing
- Works with any running ArangoDB instance

### Usage

Set up the test environment in your `TestMain`:

```go
package mypackage

import (
    "log"
    "os"
    "testing"

    "github.com/dictyBase/arangomanager/testarango"
)

var testArangoDB *testarango.TestArango

func TestMain(m *testing.M) {
    // Create a test database
    ta, err := testarango.NewTestArangoFromEnv(true)
    if err != nil {
        log.Fatalf("failed to connect to test database: %s", err)
    }
    testArangoDB = ta
    
    // Run tests
    code := m.Run()
    
    // Clean up the test database
    db, err := ta.DB(ta.Database)
    if err != nil {
        log.Fatalf("error getting database: %s", err)
    }
    if err := db.Drop(); err != nil {
        log.Fatalf("error dropping database: %s", err)
    }
    
    os.Exit(code)
}

func TestSomething(t *testing.T) {
    // Use the test database
    session, db, err := arangomanager.NewSessionDb(&arangomanager.ConnectParams{
        User:     testArangoDB.User,
        Pass:     testArangoDB.Pass,
        Host:     testArangoDB.Host,
        Port:     testArangoDB.Port,
        Database: testArangoDB.Database,
    })
    
    // Run your tests with session and db
    // ...
}
```

### Requirements

- A running ArangoDB instance
- An existing user with administrative privileges (to create test databases)
- Environment variables set:
  - `ARANGO_HOST`: ArangoDB host
  - `ARANGO_USER`: ArangoDB username
  - `ARANGO_PASS`: ArangoDB password
  - `ARANGO_PORT`: (Optional) ArangoDB port, defaults to 8529

## Advanced Usage

See the [GoDoc](https://pkg.go.dev/github.com/dictyBase/arangomanager) for full API documentation.

## License

BSD-2-Clause
