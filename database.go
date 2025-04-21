package arangomanager

import (
	"context"
	"fmt"
	"math"
	"time"

	driver "github.com/arangodb/go-driver"
)

const tranSize = 12

// TransactionOptions represents options for transaction operations
type TransactionOptions struct {
	// ReadCollections is a list of collections that will be read during the transaction
	ReadCollections []string
	// WriteCollections is a list of collections that will be written to during the transaction
	WriteCollections []string
	// ExclusiveCollections is a list of collections that will be exclusively locked during the transaction
	ExclusiveCollections []string
	// WaitForSync if set to true, will force the transaction to write all data to disk before returning
	WaitForSync bool
	// AllowImplicit if set to true, allows reading from undeclared collections (only for Transaction)
	AllowImplicit bool
	// LockTimeout the timeout for waiting on collection locks (in seconds)
	LockTimeout int
	// MaxTransactionSize the maximum size of the transaction in bytes
	MaxTransactionSize int
}

// Database struct.
type Database struct {
	dbh driver.Database
}

// DefaultTransactionOptions returns default options for transactions
func DefaultTransactionOptions() *TransactionOptions {
	return &TransactionOptions{
		MaxTransactionSize: int(math.Pow10(tranSize)),
	}
}

func (d *Database) BeginTransaction(
	ctx context.Context,
	opts *TransactionOptions,
) (*TransactionHandler, error) {
	if opts == nil {
		opts = DefaultTransactionOptions()
	}

	// Create transaction options
	beginOpts := &driver.BeginTransactionOptions{
		// Set max transaction size with safety checks
		MaxTransactionSize: uint64(0), // Default to 0
	}
	beginOpts.WaitForSync = opts.WaitForSync
	if opts.LockTimeout > 0 {
		beginOpts.LockTimeout = time.Duration(opts.LockTimeout) * time.Second
	}
	// Begin transaction
	txID, err := d.dbh.BeginTransaction(
		ctx, driver.TransactionCollections{
			Read:      opts.ReadCollections,
			Write:     opts.WriteCollections,
			Exclusive: opts.ExclusiveCollections,
		}, beginOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	// Create transaction context
	txCtx := driver.WithTransactionID(ctx, txID)
	return &TransactionHandler{
		db:       d,
		id:       txID,
		ctx:      txCtx,
		canceled: false,
	}, nil
}

// Handler returns the raw arangodb database handler.
func (d *Database) Handler() driver.Database {
	return d.dbh
}

// SearchRows query the database with bind parameters that is expected to return
// multiple rows of result.
func (d *Database) SearchRows(
	query string,
	bindVars map[string]interface{},
) (*Resultset, error) {
	// validate
	if err := d.dbh.ValidateQuery(context.Background(), query); err != nil {
		return &Resultset{
				empty: true,
			}, fmt.Errorf(
				"error in validating the query %s",
				err,
			)
	}
	ctx := context.Background()
	cqr, err := d.dbh.Query(ctx, query, bindVars)
	if err != nil {
		return &Resultset{
				empty: true,
			}, fmt.Errorf(
				"error in running search %s",
				err,
			)
	}
	if !cqr.HasMore() {
		return &Resultset{empty: true}, nil
	}

	return &Resultset{cursor: cqr, ctx: ctx}, nil
}

// Search query the database that is expected to return multiple rows of result.
func (d *Database) Search(query string) (*Resultset, error) {
	return d.SearchRows(query, nil)
}

// CountWithParams query the database with bind parameters that is expected to
// return count of result.
func (d *Database) CountWithParams(
	query string,
	bindVars map[string]interface{},
) (int64, error) {
	// validate
	if err := d.dbh.ValidateQuery(context.Background(), query); err != nil {
		return 0, fmt.Errorf("error in validating the query %s", err)
	}
	cobj, err := d.dbh.Query(
		driver.WithQueryCount(context.Background(), true),
		query,
		bindVars,
	)
	if err != nil {
		return 0, fmt.Errorf("error with query %s", err)
	}

	return cobj.Count(), nil
}

// Count query the database that is expected to return count of result.
func (d *Database) Count(query string) (int64, error) {
	return d.CountWithParams(query, nil)
}

// Exec is to run data modification query that is not expected to return any
// result.
func (d *Database) Exec(query string) error {
	return d.Do(query, nil)
}

// Do is to run data modification query with bind parameters that is not
// expected to return any result.
func (d *Database) Do(query string, bindVars map[string]interface{}) error {
	ctx := driver.WithSilent(context.Background())
	_, err := d.dbh.Query(ctx, query, bindVars)
	if err != nil {
		return fmt.Errorf("error in data modification query %s", err)
	}

	return nil
}

// GetRow query the database with bind parameters that is expected to return
// single row of result.
func (d *Database) GetRow(
	query string,
	bindVars map[string]interface{},
) (*Result, error) {
	if err := d.dbh.ValidateQuery(context.Background(), query); err != nil {
		return &Result{
				empty: true,
			}, fmt.Errorf(
				"error in validating the query %s",
				err,
			)
	}
	cqr, err := d.dbh.Query(context.Background(), query, bindVars)

	return d.getResult(cqr, err)
}

// DoRun is to run data modification query with bind parameters
// that is expected to return a result. It is an alias for GetRow.
func (d *Database) DoRun(
	query string,
	bindVars map[string]interface{},
) (*Result, error) {
	return d.GetRow(query, bindVars)
}

// Get query the database to return single row of result.
func (d *Database) Get(query string) (*Result, error) {
	return d.GetRow(query, nil)
}

// Run is to run data modification query that is expected to return a result
// It is a convenient alias for Get method.
func (d *Database) Run(query string) (*Result, error) {
	return d.GetRow(query, nil)
}

// Collection returns collection attached to current database.
func (d *Database) Collection(name string) (driver.Collection, error) {
	var coll driver.Collection
	ok, err := d.dbh.CollectionExists(context.Background(), name)
	if err != nil {
		return coll, fmt.Errorf("unable to check for collection %s", name)
	}
	if !ok {
		return coll, fmt.Errorf("collection %s has to be created", name)
	}
	coll, err = d.dbh.Collection(context.Background(), name)
	if err != nil {
		return coll, fmt.Errorf("error in getting collection %s", err)
	}

	return coll, nil
}

// CreateCollection creates a collection in the database.
func (d *Database) CreateCollection(
	name string,
	opt *driver.CreateCollectionOptions,
) (driver.Collection, error) {
	var coll driver.Collection
	ok, err := d.dbh.CollectionExists(context.Background(), name)
	if err != nil {
		return coll, fmt.Errorf("error in collection lookup %s", err)
	}
	if ok {
		return coll, fmt.Errorf("collection %s exists", name)
	}
	coll, err = d.dbh.CreateCollection(context.TODO(), name, opt)
	if err != nil {
		return coll, fmt.Errorf("error in creating collection %s", err)
	}

	return coll, nil
}

// FindOrCreateCollection finds or creates a collection in the database. The
// method is expected to be called by the user who has privileges to create the
// collection.
func (d *Database) FindOrCreateCollection(
	name string,
	opt *driver.CreateCollectionOptions,
) (driver.Collection, error) {
	var coll driver.Collection
	ok, err := d.dbh.CollectionExists(context.Background(), name)
	if err != nil {
		return coll, fmt.Errorf("unable to check for collection %s", name)
	}
	if ok {
		coll, err = d.dbh.Collection(context.Background(), name)
		if err != nil {
			return coll, fmt.Errorf("error in fetching collection %s", err)
		}

		return coll, nil
	}
	coll, err = d.dbh.CreateCollection(context.TODO(), name, opt)
	if err != nil {
		return coll, fmt.Errorf("error in creating collection %s", err)
	}

	return coll, nil
}

// FindOrCreateGraph finds or creates a named graph in the database.
func (d *Database) FindOrCreateGraph(
	name string,
	defs []driver.EdgeDefinition,
) (driver.Graph, error) {
	var grph driver.Graph
	ok, err := d.dbh.GraphExists(context.Background(), name)
	if err != nil {
		return grph, fmt.Errorf("error in graph %s lookup %s", name, err)
	}
	if ok {
		grph, err = d.dbh.Graph(context.Background(), name)
		if err != nil {
			return grph, fmt.Errorf("error in fetching graph %s", err)
		}

		return grph, nil
	}
	grph, err = d.dbh.CreateGraphV2(
		context.Background(),
		name,
		&driver.CreateGraphOptions{EdgeDefinitions: defs},
	)
	if err != nil {
		return grph, fmt.Errorf("error in creating graph %s", err)
	}

	return grph, nil
}

// EnsureGeoIndex finds or creates a geo index on a specified collection.
func (d *Database) EnsureGeoIndex(
	coll string, fields []string,
	opts *driver.EnsureGeoIndexOptions,
) (driver.Index, bool, error) {
	var idx driver.Index
	cobj, err := d.Collection(coll)
	if err != nil {
		return idx, false, fmt.Errorf("unable to check for collection %s", coll)
	}
	idx, isOk, err := cobj.EnsureGeoIndex(context.Background(), fields, opts)
	if err != nil {
		return idx, isOk, fmt.Errorf("error in handling index %s", err)
	}

	return idx, isOk, nil
}

// EnsureHashIndex finds or creates a hash index on a specified collection.
func (d *Database) EnsureHashIndex(
	coll string, fields []string,
	opts *driver.EnsureHashIndexOptions,
) (driver.Index, bool, error) {
	var idx driver.Index
	cobj, err := d.Collection(coll)
	if err != nil {
		return idx, false, fmt.Errorf("unable to check for collection %s", coll)
	}
	idx, isOk, err := cobj.EnsureHashIndex(context.Background(), fields, opts)
	if err != nil {
		return idx, isOk, fmt.Errorf("error in handling index %s", err)
	}

	return idx, isOk, nil
}

// EnsurePersistentIndex finds or creates a persistent index on a specified collection.
func (d *Database) EnsurePersistentIndex(
	coll string, fields []string,
	opts *driver.EnsurePersistentIndexOptions,
) (driver.Index, bool, error) {
	var idx driver.Index
	cobj, err := d.Collection(coll)
	if err != nil {
		return idx, false, fmt.Errorf("unable to check for collection %s", coll)
	}
	idx, isOk, err := cobj.EnsurePersistentIndex(
		context.Background(),
		fields,
		opts,
	)
	if err != nil {
		return idx, isOk, fmt.Errorf("error in handling index %s", err)
	}

	return idx, isOk, nil
}

// EnsureSkipListIndex finds or creates a skip list index on a specified collection.
func (d *Database) EnsureSkipListIndex(
	coll string, fields []string,
	opts *driver.EnsureSkipListIndexOptions,
) (driver.Index, bool, error) {
	var idx driver.Index
	cobj, err := d.Collection(coll)
	if err != nil {
		return idx, false, fmt.Errorf("unable to check for collection %s", coll)
	}
	idx, isOk, err := cobj.EnsureSkipListIndex(
		context.Background(),
		fields,
		opts,
	)
	if err != nil {
		return idx, isOk, fmt.Errorf("error in handling index %s", err)
	}

	return idx, isOk, nil
}

// Drop removes the database.
func (d *Database) Drop() error {
	if err := d.dbh.Remove(context.Background()); err != nil {
		return fmt.Errorf("error in removing database %s", err)
	}

	return nil
}

// ValidateQ validates the query.
func (d *Database) ValidateQ(q string) error {
	if err := d.dbh.ValidateQuery(context.Background(), q); err != nil {
		return fmt.Errorf("error in validating the query %s", err)
	}

	return nil
}

// Truncate removes all data from the collections without touching the indexes.
func (d *Database) Truncate(names ...string) error {
	for _, n := range names {
		if _, err := d.Collection(n); err != nil {
			return err
		}
	}
	_, err := d.dbh.Transaction(
		context.Background(),
		truncateFn,
		&driver.TransactionOptions{
			WriteCollections: names,
			ReadCollections:  names,
			Params:           []interface{}{names},
			MaxTransactionSize: func() int {
				size := math.Pow10(tranSize)
				if size > float64(math.MaxInt) {
					return math.MaxInt
				}
				return int(size)
			}(),
		})
	if err != nil {
		return fmt.Errorf("error in truncating collections %s", err)
	}

	return nil
}

func (d *Database) getResult(cdr driver.Cursor, err error) (*Result, error) {
	if err != nil {
		return &Result{empty: true}, fmt.Errorf("error in query %s", err)
	}
	if !cdr.HasMore() {
		return &Result{empty: true}, nil
	}

	return &Result{cursor: cdr}, nil
}
