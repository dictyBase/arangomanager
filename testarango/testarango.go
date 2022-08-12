package testarango

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/arangomanager"
)

const (
	aPort  = 8529
	minLen = 6
	maxLen = 8
)

// CheckArangoEnv checks for the presence of the following
// environment variables
//
//	ARANGO_HOST
//	ARANGO_USER
//	ARANGO_PASS
func CheckArangoEnv() error {
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

// Generates a random string between a range(min and max) of length.
func RandomString(min, max int) string {
	alphanum := []byte("abcdefghijklmnopqrstuvwxyz")
	rand.Seed(time.Now().UTC().UnixNano())
	size := min + rand.Intn(max-min)
	byt := make([]byte, size)
	alen := len(alphanum)
	for i := 0; i < size; i++ {
		pos := rand.Intn(alen)
		byt[i] = alphanum[pos]
	}

	return string(byt)
}

// TestArango is a container for managing a disposable database
// instance.
type TestArango struct {
	*arangomanager.ConnectParams
	*arangomanager.Session
}

// NewTestArangoFromEnv is a constructor for TestArango instance.
// It expects the following environmental variables to be set.
//
//	ARANGO_HOST
//	ARANGO_USER
//	ARANGO_PASS
//	ARANGO_PORT is optional, by default it uses 8529
//
// isCreate toggles whether a random disposable test database
// will be created during instantiation. It is false by default.
func NewTestArangoFromEnv(isCreate bool) (*TestArango, error) {
	tra := new(TestArango)
	if err := CheckArangoEnv(); err != nil {
		return tra, err
	}
	tra.ConnectParams = &arangomanager.ConnectParams{
		User: os.Getenv("ARANGO_USER"),
		Pass: os.Getenv("ARANGO_PASS"),
		Host: os.Getenv("ARANGO_HOST"),
		Port: aPort,
	}
	if len(os.Getenv("ARANGO_PORT")) > 0 {
		aport, _ := strconv.Atoi(os.Getenv("ARANGO_PORT"))
		tra.ConnectParams.Port = aport
	}
	sess, err := arangomanager.Connect(
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
	if isCreate {
		if err := tra.CreateTestDb(RandomString(minLen, maxLen), &driver.CreateDatabaseOptions{}); err != nil {
			return tra, err
		}
	}

	return tra, nil
}

// NewTestArango is a constructor for TestArango instance from the given
// database credentials.
// isCreate toggles whether a random disposable test
// database will be created during instantiation. It is false by default.
func NewTestArango(
	user, pass, host string,
	port int,
	isCreate bool,
) (*TestArango, error) {
	tra := new(TestArango)
	tra.ConnectParams = &arangomanager.ConnectParams{
		User: user,
		Pass: pass,
		Host: host,
		Port: port,
	}
	sess, err := arangomanager.Connect(
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
	if isCreate {
		if err := tra.CreateTestDb(RandomString(minLen, maxLen), &driver.CreateDatabaseOptions{}); err != nil {
			return tra, err
		}
	}

	return tra, nil
}

// CreateTestDb creates a test database of given name.
func (ta *TestArango) CreateTestDb(
	name string,
	opt *driver.CreateDatabaseOptions,
) error {
	if err := ta.CreateDB(name, opt); err != nil {
		return err
	}
	ta.Database = name

	return nil
}
