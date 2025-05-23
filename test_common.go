package arangomanager

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	driver "github.com/arangodb/go-driver"
	"github.com/stretchr/testify/require"
)

const (
	genderQ = `
		FOR d IN @@collection 
			FILTER d.gender == @gender
			RETURN d
	`
	genderQNoParam = `
		FOR d IN %s 
			FILTER d.gender == '%s'
			RETURN d
	`
	userQ = `
		FOR d in @@collection 
			FILTER d.name.first == @first
			FILTER d.name.last == @last
			RETURN d
	`
	userIns = `
		INSERT {
			name: {
				first: @first,
				last: @last
			},
			gender: @gender,
			contact: {
				region: @region,
				address: {
					city: @city,
					state: @state,
					zip: @zip
				}
			}
		} INTO %s
	`
	aPort  = 8529
	minLen = 10
	maxLen = 15
)

// DocParams defines parameters for document operations
type DocParams struct {
	T         *testing.T
	TX        *TransactionHandler
	Coll      driver.Collection
	FirstName string
	LastName  string
}

// TxParams defines parameters for creating a test transaction
type TxParams struct {
	T        *testing.T
	DB       *Database
	Coll     driver.Collection
	ReadOnly bool
}

// DocExistsParams defines parameters for checking document existence
type DocExistsParams struct {
	T           *testing.T
	DB          *Database
	Coll        driver.Collection
	FirstName   string
	LastName    string
	ShouldExist bool
}

func randomIntInRange(min, max int) (int, error) {
	if min >= max {
		return 0, fmt.Errorf("Invalid range")
	}
	// Calculate the number of possible values within the range
	possibleValues := big.NewInt(int64(max - min))
	// Generate a random number using crypto/rand
	randomValue, err := rand.Int(rand.Reader, possibleValues)
	if err != nil {
		return 0, err
	}
	// Add the minimum value to the random number
	return min + int(randomValue.Int64()), nil
}

// Generate a random number using crypto/rand.
func RandomInt(num int) (int, error) {
	randomValue, err := rand.Int(rand.Reader, big.NewInt(int64(num)))
	if err != nil {
		return 0, err
	}
	return int(randomValue.Int64()), nil
}

func FixedLenRandomString(length int) string {
	alphanum := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	byt := make([]byte, 0)
	alen := len(alphanum)
	for i := 0; i < length; i++ {
		pos, _ := RandomInt(alen)
		byt = append(byt, alphanum[pos])
	}

	return string(byt)
}

// Generates a random string between a range(min and max) of length.
func RandomString(min, max int) string {
	alphanum := []byte("abcdefghijklmnopqrstuvwxyz")
	size, _ := randomIntInRange(min, max)
	byt := make([]byte, size)
	alen := len(alphanum)
	for i := 0; i < size; i++ {
		pos, _ := RandomInt(alen)
		byt[i] = alphanum[pos]
	}

	return string(byt)
}

type testArango struct {
	*ConnectParams
	*Session
}

type testUserDb struct {
	driver.DocumentMeta
	Birthday *time.Time `json:"birthday"`
	Contact  struct {
		Address struct {
			City   string `json:"city"`
			State  string `json:"state"`
			Street string `json:"street"`
			Zip    string `json:"zip"`
		} `json:"address"`
		Email  []string `json:"email"`
		Phone  []string `json:"phone"`
		Region string   `json:"region"`
	} `json:"contact"`
	Gender      string     `json:"gender"`
	Likes       []string   `json:"likes"`
	MemberSince *time.Time `json:"memberSince"`
	Name        struct {
		First string `json:"first"`
		Last  string `json:"last"`
	} `json:"name"`
}

type testUser struct {
	driver.DocumentMeta
	Birthday *userDate `json:"birthday"`
	Contact  struct {
		Address struct {
			City   string `json:"city"`
			State  string `json:"state"`
			Street string `json:"street"`
			Zip    string `json:"zip"`
		} `json:"address"`
		Email  []string `json:"email"`
		Phone  []string `json:"phone"`
		Region string   `json:"region"`
	} `json:"contact"`
	Gender      string    `json:"gender"`
	Likes       []string  `json:"likes"`
	MemberSince *userDate `json:"memberSince"`
	Name        struct {
		First string `json:"first"`
		Last  string `json:"last"`
	} `json:"name"`
}

type userDate struct {
	time.Time
}

func (ud *userDate) UnmarshalJSON(in []byte) error {
	t, err := time.Parse("2006-01-02", strings.Trim(string(in), `"`))
	if err != nil {
		return fmt.Errorf("error in parsing time %s", err)
	}
	ud.Time = t

	return nil
}

func checkArangoEnv() error {
	envs := []string{
		"ARANGO_USER",
		"ARANGO_HOST",
		"ARANGO_PASS",
	}
	for _, e := range envs {
		if len(os.Getenv(e)) == 0 {
			return fmt.Errorf("env %s is not set", e)
		}
	}

	return nil
}

func teardown(t *testing.T, c driver.Collection) {
	t.Helper()
	if err := c.Remove(context.Background()); err != nil {
		t.Fatalf("unable to truncate collection %s %s", c.Name(), err)
	}
}

func setup(t *testing.T, db *Database) driver.Collection {
	t.Helper()
	coll, err := db.FindOrCreateCollection(
		RandomString(minLen, maxLen),
		&driver.CreateCollectionOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = loadTestData(coll); err != nil {
		t.Fatal(err)
	}

	return coll
}

func newTestArangoFromEnv(isCreate bool) (*testArango, error) {
	tra := new(testArango)
	if err := checkArangoEnv(); err != nil {
		return tra, err
	}
	tra.ConnectParams = &ConnectParams{
		User: os.Getenv("ARANGO_USER"),
		Pass: os.Getenv("ARANGO_PASS"),
		Host: os.Getenv("ARANGO_HOST"),
		Port: aPort,
	}
	if len(os.Getenv("ARANGO_PORT")) > 0 {
		aport, _ := strconv.Atoi(os.Getenv("ARANGO_PORT"))
		tra.ConnectParams.Port = aport
	}
	sess, err := Connect(
		tra.ConnectParams.Host,
		tra.ConnectParams.User,
		tra.ConnectParams.Pass,
		tra.ConnectParams.Port,
		false,
	)
	if err != nil {
		return tra, err
	}
	tra.Session = sess
	tra.Database = RandomString(minLen, maxLen)
	if isCreate {
		if err := sess.CreateDB(tra.Database, &driver.CreateDatabaseOptions{}); err != nil {
			return tra, err
		}
	}

	return tra, nil
}

func getReader() (io.Reader, error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	dir, err := os.Getwd()
	if err != nil {
		return buff, fmt.Errorf("unable to get current dir %s", err)
	}
	fhr, err := os.Open(
		filepath.Join(
			dir, "testdata", "names.json",
		),
	)
	if err != nil {
		return fhr, fmt.Errorf("error in opening file %s", err)
	}

	return fhr, nil
}

func loadTestData(coll driver.Collection) error {
	reader, err := getReader()
	if err != nil {
		return err
	}
	dec := json.NewDecoder(reader)
	var ausr []*testUser
	for {
		var usr *testUser
		if err := dec.Decode(&usr); err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error in decoding %s", err)
		}
		ausr = append(ausr, usr)
	}
	_, err = coll.ImportDocuments(
		context.Background(),
		ausr,
		&driver.ImportDocumentOptions{Complete: true},
	)
	if err != nil {
		return fmt.Errorf("error in importing document %s", err)
	}

	return nil
}

// setupTestTx sets up the test environment and returns database and collection objects
func setupTestTx(t *testing.T) (*Database, driver.Collection, func()) {
	t.Helper()
	// Setup test environment
	ta, err := newTestArangoFromEnv(true)
	if err != nil {
		t.Fatalf("failed to create test database: %s", err)
	}

	// Create cleanup function
	cleanup := func() {
		// Clean up the database
		dbh, _ := ta.Session.client.Database(context.Background(), ta.Database)
		if dbh != nil {
			if err := dbh.Remove(context.Background()); err != nil {
				t.Logf("failed to drop test database: %s", err)
			}
		}
	}

	db, err := ta.Session.DB(ta.Database)
	if err != nil {
		cleanup()
		t.Fatalf("failed to get database: %s", err)
	}

	// Create a test collection
	coll := setup(t, db)

	return db, coll, cleanup
}

// beginTestTransaction creates a transaction with default options for testing
func beginTestTransaction(params TxParams) *TransactionHandler {
	params.T.Helper()

	opts := &TransactionOptions{}
	if params.ReadOnly {
		opts.ReadCollections = []string{params.Coll.Name()}
	} else {
		opts.WriteCollections = []string{params.Coll.Name()}
	}

	tx, err := params.DB.BeginTransaction(context.Background(), opts)
	if err != nil {
		params.T.Fatalf("failed to begin transaction: %s", err)
	}

	return tx
}

// assertTxCanceled checks if a transaction is canceled as expected
func assertTxCanceled(
	t *testing.T,
	tx *TransactionHandler,
	expectedCanceled bool,
) {
	t.Helper()
	assert := require.New(t)
	assert.Equal(expectedCanceled, tx.canceled,
		"Transaction canceled state mismatch, expected: %v, got: %v",
		expectedCanceled, tx.canceled)
}

// insertTestDocument inserts a test document using the provided transaction
func insertTestDocument(params DocParams) {
	params.T.Helper()
	assert := require.New(params.T)

	query := fmt.Sprintf(userIns, params.Coll.Name())
	bindVars := map[string]interface{}{
		"first":  params.FirstName,
		"last":   params.LastName,
		"gender": "male",
		"region": "test",
		"city":   "TestCity",
		"state":  "TestState",
		"zip":    "12345",
	}

	err := params.TX.Do(query, bindVars)
	assert.NoError(err)
}

// assertDocumentExists checks if a document exists in the database
func assertDocumentExists(params DocExistsParams) {
	params.T.Helper()
	assert := require.New(params.T)

	result, err := params.DB.GetRow(
		fmt.Sprintf(
			"FOR d IN %s FILTER d.name.first == @first AND d.name.last == @last RETURN d",
			params.Coll.Name(),
		),
		map[string]interface{}{
			"first": params.FirstName,
			"last":  params.LastName,
		},
	)
	assert.NoError(err)

	if params.ShouldExist {
		assert.False(
			result.IsEmpty(),
			"Document should exist but was not found: %s %s",
			params.FirstName,
			params.LastName,
		)
	} else {
		assert.True(result.IsEmpty(),
			"Document should not exist but was found: %s %s", params.FirstName, params.LastName)
	}
}
