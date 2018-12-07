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

// CheckArangoEnv checks for the presence of the following
// environment variables
//   ARANGO_HOST
//   ARANGO_USER
//   ARANGO_PASS
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

// Generates a random string between a range(min and max) of length
func RandomString(min, max int) string {
	alphanum := []byte("abcdefghijklmnopqrstuvwxyz")
	rand.Seed(time.Now().UTC().UnixNano())
	size := min + rand.Intn(max-min)
	b := make([]byte, size)
	alen := len(alphanum)
	for i := 0; i < size; i++ {
		pos := rand.Intn(alen)
		b[i] = alphanum[pos]
	}
	return string(b)
}

// TestArango is a container for managing a disposable database
// instance
type TestArango struct {
	*arangomanager.ConnectParams
	*arangomanager.Session
}

// NewTestArangoFromEnv is a constructor for TestArango instance.
// It expects the following environmental variables to be set.
//   ARANGO_HOST
//   ARANGO_USER
//   ARANGO_PASS
//   ARANGO_PORT is optional, by default it uses 8529
// isCreate toggles whether a random disposable test database
// will be created during instantiation. It is false by default.
func NewTestArangoFromEnv(isCreate bool) (*TestArango, error) {
	ta := new(TestArango)
	if err := CheckArangoEnv(); err != nil {
		return ta, err
	}
	ta.ConnectParams = &arangomanager.ConnectParams{
		User: os.Getenv("ARANGO_USER"),
		Pass: os.Getenv("ARANGO_PASS"),
		Host: os.Getenv("ARANGO_HOST"),
		Port: 8529,
	}
	if len(os.Getenv("ARANGO_PORT")) > 0 {
		aport, _ := strconv.Atoi(os.Getenv("ARANGO_PORT"))
		ta.ConnectParams.Port = aport
	}
	sess, err := arangomanager.Connect(
		ta.ConnectParams.Host,
		ta.ConnectParams.User,
		ta.ConnectParams.Pass,
		ta.ConnectParams.Port,
		false,
	)
	if err != nil {
		return ta, err
	}
	ta.Session = sess
	if isCreate {
		if err := ta.CreateTestDb(RandomString(6, 8), &driver.CreateDatabaseOptions{}); err != nil {
			return ta, err
		}
	}
	return ta, nil
}

// NewTestArango is a constructor for TestArango instance from the given
// database credentials.
// isCreate toggles whether a random disposable test
// database will be created during instantiation. It is false by default.
func NewTestArango(user, pass, host string, port int, isCreate bool) (*TestArango, error) {
	ta := new(TestArango)
	ta.ConnectParams = &arangomanager.ConnectParams{
		User: user,
		Pass: pass,
		Host: host,
		Port: port,
	}
	sess, err := arangomanager.Connect(
		ta.ConnectParams.Host,
		ta.ConnectParams.User,
		ta.ConnectParams.Pass,
		ta.ConnectParams.Port,
		false,
	)
	if err != nil {
		return ta, err
	}
	ta.Session = sess
	if isCreate {
		if err := ta.CreateTestDb(RandomString(6, 8), &driver.CreateDatabaseOptions{}); err != nil {
			return ta, err
		}
	}
	return ta, nil
}

// CreateTestDb creates a test database of given name
func (ta *TestArango) CreateTestDb(name string, opt *driver.CreateDatabaseOptions) error {
	err := ta.CreateDB(name, opt)
	if err != nil {
		return err
	}
	ta.Database = name
	return nil
}
