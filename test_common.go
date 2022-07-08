package arangomanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	driver "github.com/arangodb/go-driver"
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

// Generates a random string between a range(min and max) of length.
func randomString(min, max int) string {
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

func teardown(t *testing.T, c driver.Collection) {
	t.Helper()
	if err := c.Remove(context.Background()); err != nil {
		t.Fatalf("unable to truncate collection %s %s", c.Name(), err)
	}
}

func setup(t *testing.T, db *Database) driver.Collection {
	t.Helper()
	coll, err := db.FindOrCreateCollection(randomString(minLen, maxLen), &driver.CreateCollectionOptions{})
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
	tra.Database = randomString(minLen, maxLen)
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
	_, err = coll.ImportDocuments(context.Background(), ausr, &driver.ImportDocumentOptions{Complete: true})
	if err != nil {
		return fmt.Errorf("error in importing document %s", err)
	}

	return nil
}
