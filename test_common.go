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
	if err := c.Remove(context.Background()); err != nil {
		t.Fatalf("unable to truncate collection %s %s", c.Name(), err)
	}
}

func setup(db *Database, t *testing.T) driver.Collection {
	c, err := db.FindOrCreateCollection(randomString(10, 15), &driver.CreateCollectionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = loadTestData(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func newTestArangoFromEnv(isCreate bool) (*testArango, error) {
	ta := new(testArango)
	if err := checkArangoEnv(); err != nil {
		return ta, err
	}
	ta.ConnectParams = &ConnectParams{
		User: os.Getenv("ARANGO_USER"),
		Pass: os.Getenv("ARANGO_PASS"),
		Host: os.Getenv("ARANGO_HOST"),
		Port: 8529,
	}
	if len(os.Getenv("ARANGO_PORT")) > 0 {
		aport, _ := strconv.Atoi(os.Getenv("ARANGO_PORT"))
		ta.ConnectParams.Port = aport
	}
	sess, err := Connect(
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
	ta.Database = randomString(6, 8)
	if isCreate {
		if err := sess.CreateDB(ta.Database, &driver.CreateDatabaseOptions{}); err != nil {
			return ta, err
		}
	}
	return ta, nil
}

func getReader() (io.Reader, error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	dir, err := os.Getwd()
	if err != nil {
		return buff, fmt.Errorf("unable to get current dir %s", err)
	}
	return os.Open(
		filepath.Join(
			dir, "testdata", "names.json",
		),
	)
}

func loadTestData(c driver.Collection) error {
	reader, err := getReader()
	if err != nil {
		return err
	}
	dec := json.NewDecoder(reader)
	var au []*testUser
	for {
		var u *testUser
		if err := dec.Decode(&u); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		au = append(au, u)
	}
	_, err = c.ImportDocuments(context.Background(), au, &driver.ImportDocumentOptions{Complete: true})
	return err
}
