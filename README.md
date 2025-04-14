# ArangoManager

[![License](https://img.shields.io/badge/License-BSD%202--Clause-blue.svg)](LICENSE)   
![Continuous integration](https://github.com/dictyBase/arangomanager/workflows/Continuous%20integration/badge.svg)
[![GoDoc](https://pkg.go.dev/badge/github.com/dictyBase/arangomanager)](https://pkg.go.dev/github.com/dictyBase/arangomanager)
[![codecov](https://codecov.io/gh/dictyBase/arangomanager/branch/develop/graph/badge.svg)](https://codecov.io/gh/dictyBase/arangomanager)

A Go library providing utilities and abstractions for working with [ArangoDB](https://arangodb.com).

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Main Components](#main-components)
  - [Session](#session)
  - [Database](#database)
  - [ResultSet](#resultset)
  - [Result](#result)
- [Testing with TestArango](#testing-with-testarango)
- [Query Package](#query-package)
- [Collection Package](#collection-package)
- [Command Line Integration](#command-line-integration)
  - [Flag Package](#flag-package)
- [Advanced Usage](#advanced-usage)
- [License](#license)

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

## Query Package

The `query` package provides powerful utilities for building ArangoDB Query Language (AQL) filter statements, especially for handling complex filtering requirements with logical operations.

### Features

- Parse filter strings into structured Filter objects
- Generate AQL filter statements with support for:
  - Standard comparison operators (`==`, `!=`, `>`, `<`, `>=`, `<=`)
  - String pattern matching (`=~`, `!~`)
  - Date comparison operators (prefixed with `$`, e.g., `$==`, `$>`)
  - Array operation operators (prefixed with `@`, e.g., `@==`, `@=~`)
  - Complex logical expressions with AND/OR operations

### Usage

#### Parsing Filter Strings

```go
// Parse a filter string with multiple conditions
// Format: field operator value[logic]
// where logic is "," for OR and ";" for AND
filters, err := query.ParseFilterString("created_at$>=2023-01-01;status==active,status==pending")
if err != nil {
    // handle error
}
```

#### Generating AQL Filter Statements

```go
// Create a field mapping for the query
fieldMap := map[string]string{
    "created_at": "doc.created_at",
    "status": "doc.status",
}

// Generate AQL filter statement with qualified field names
aqlStatement, err := query.GenQualifiedAQLFilterStatement(fieldMap, filters)
if err != nil {
    // handle error
}

// Use the filter in your AQL query
queryString := fmt.Sprintf(`
    FOR doc IN collection
        %s
        RETURN doc
`, aqlStatement)

// Execute the query with the database
resultset, err := db.SearchRows(queryString, nil)
```

#### Using Statement Parameters

```go
// For more control, use StatementParameters
params := &query.StatementParameters{
    Fmap: fieldMap,
    Filters: filters,
    Doc: "doc",  // document variable name in FOR loop
}

// Generate AQL filter statement
aqlStatement, err := query.GenAQLFilterStatement(params)
if err != nil {
    // handle error
}
```

#### Supported Operators

| Type | Operators | Example |
|------|-----------|---------|
| Standard | `==`, `!=`, `>`, `<`, `>=`, `<=` | `status==active` |
| String | `=~` (contains), `!~` (not contains) | `name=~John` |
| Date | `$==`, `$>`, `$<`, `$>=`, `$<=` | `created_at$>=2023-01-01` |
| Array | `@==`, `@!=`, `@=~`, `@!~` | `tags@==important` |

#### Logical Operations

- Use `,` between conditions for OR logic
- Use `;` between conditions for AND logic

Example:
```
status==active;created_at$>=2023-01,created_at$<=2023-12
```

This translates to: `(status equals "active") AND ((created_at >= 2023-01) OR (created_at <= 2023-12))`

## Collection Package

The `collection` package provides functional programming utilities for working
with slices and collections in Go. These utilities enable more expressive and
concise code when manipulating data.

### Features

- Generic collection functions with full type safety
- Functional programming patterns like map, filter, and reduce
- Support for sequences and iterators
- Curried functions for partial application and composition
- Tuple types for multi-value returns

### Usage Examples

#### Transforming Data with Map

```go
// Transform a slice of strings to uppercase
names := []string{"john", "alice", "bob"}
upperNames := collection.Map(names, strings.ToUpper)
// Result: ["JOHN", "ALICE", "BOB"]
```

#### Filtering Data

```go
// Filter a slice to only include even numbers
numbers := []int{1, 2, 3, 4, 5, 6, 7, 8}
evenNumbers := collection.Filter(numbers, func(n int) bool {
    return n%2 == 0
})
// Result: [2, 4, 6, 8]
```

#### Partitioning Data

```go
// Partition users by age (adults and minors)
users := []User{
    {Name: "John", Age: 30},
    {Name: "Alice", Age: 16},
    {Name: "Bob", Age: 25},
    {Name: "Emma", Age: 17},
}

isAdult := func(u User) bool { return u.Age >= 18 }
adults, minors := collection.Partition(users, isAdult)
```

#### Function Composition with Pipe

```go
// Create a pipeline of operations
result := collection.Pipe3(
    inputData,
    func(data []int) []int { return collection.Filter(data, isEven) },
    func(data []int) []int { return collection.Map(data, multiply) },
    func(data []int) int { return sum(data) },
)
```

#### Using Tuples for Multi-Value Returns

```go
// Create and use a tuple
userStats := collection.NewTuple2("active_users", 1250)
fmt.Printf("Metric: %s, Value: %d\n", userStats.First, userStats.Second)
```

### Available Functions

- **Map**: Transform elements using a mapping function
- **Filter**: Select elements that match a predicate
- **Partition**: Split a collection based on a predicate
- **Include**: Check if an element exists in a collection
- **Pipe2/3/4**: Create functional pipelines
- **MapSeq**: Transform iterator sequences
- **RemoveStringItems**: Remove specific strings from a slice
- **IsEmpty**: Check if a collection is empty
- **CurriedXXX**: Curried versions of the above functions for partial application

## Command Line Integration

### Flag Package

The `command/flag` package provides convenient CLI flag definitions for ArangoDB connections, making it easy to integrate ArangoManager with command-line applications built with [urfave/cli](https://github.com/urfave/cli).

#### Features

- Pre-defined flag sets for ArangoDB connection parameters
- Environment variable support for all parameters
- Sensible defaults for common settings
- Compatible with urfave/cli command-line application framework

#### Usage

```go
package main

import (
    "fmt"
    "log"
    "os"
    
    "github.com/dictyBase/arangomanager"
    "github.com/dictyBase/arangomanager/command/flag"
    "github.com/urfave/cli"
)

func main() {
    app := cli.NewApp()
    app.Name = "my-arango-app"
    app.Usage = "Example application using ArangoDB"
    
    // Add ArangoDB flags to your application
    app.Flags = flag.ArangodbFlags()
    
    app.Action = func(c *cli.Context) error {
        // Parse connection parameters from CLI flags
        connParams := &arangomanager.ConnectParams{
            User:     c.String("arangodb-user"),
            Pass:     c.String("arangodb-pass"),
            Database: c.String("arangodb-database"),
            Host:     c.String("arangodb-host"),
            Port:     c.Int("arangodb-port"),
            Istls:    c.Bool("is-secure"),
        }
        
        // Connect to ArangoDB
        session, db, err := arangomanager.NewSessionDb(connParams)
        if err != nil {
            return cli.NewExitError(fmt.Sprintf("failed to connect: %s", err), 1)
        }
        
        // Your application logic here...
        fmt.Printf("Connected to database: %s\n", c.String("arangodb-database"))
        
        return nil
    }
    
    if err := app.Run(os.Args); err != nil {
        log.Fatal(err)
    }
}
```

#### Available Flag Sets

The package provides two main flag sets:

1. **ArangoFlags()** - Basic connection flags:
   - `--arangodb-pass, --pass` (required): ArangoDB password, can be set via `ARANGODB_PASS` env var
   - `--arangodb-user, --user` (required): ArangoDB username, can be set via `ARANGODB_USER` env var
   - `--arangodb-host, --host` (default: "arangodb"): ArangoDB host, can be set via `ARANGODB_SERVICE_HOST` env var
   - `--arangodb-port` (default: "8529"): ArangoDB port, can be set via `ARANGODB_SERVICE_PORT` env var
   - `--is-secure`: Flag for secured endpoint

2. **ArangodbFlags()** - Extended flags that include all basic flags plus:
   - `--arangodb-database, --db` (required): ArangoDB database name, can be set via `ARANGODB_DATABASE` env var
   - Sets `--is-secure` to true by default

## Advanced Usage

See the [GoDoc](https://pkg.go.dev/github.com/dictyBase/arangomanager) for full API documentation.

## License

BSD-2-Clause
