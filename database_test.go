package arangomanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arangodb/go-driver"
	"github.com/stretchr/testify/assert"
)

var ahost, aport, auser, apass, adb string
var adbh *Database

const (
	genderCount = `
		FOR d IN @@collection 
			FILTER d.gender == @gender
			RETURN d
	`
	genderCountS = `
		FOR d IN %s 
			FILTER d.gender == '%s'
			RETURN d
	`
)

type testArango struct {
	*ConnectParams
	*Session
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

// Generates a random string between a range(min and max) of length
func randomString(min, max int) string {
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

func TestMain(m *testing.M) {
	ta, err := newTestArangoFromEnv(true)
	if err != nil {
		log.Fatalf("unable to construct new TestArango instance %s", err)
	}
	dbh, err := ta.DB(ta.Database)
	if err != nil {
		log.Fatalf("unable to get database %s", err)
	}
	auser = ta.User
	apass = ta.Pass
	ahost = ta.Host
	aport = strconv.Itoa(ta.Port)
	adb = ta.Database
	adbh = dbh
	code := m.Run()
	if err := dbh.Drop(); err != nil {
		log.Fatalf("error in dropping database %s", err)
	}
	os.Exit(code)
}

func teardown(c driver.Collection, t *testing.T) {
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

func TestCount(t *testing.T) {
	c := setup(adbh, t)
	defer teardown(c, t)
	fc, err := adbh.Count(
		fmt.Sprintf(genderCountS, c.Name(), "female"),
	)
	if err != nil {
		t.Fatalf("error in running counting query %s", err)
	}
	assert := assert.New(t)
	assert.Equalf(fc, int64(15), "expect %d received %d", 15, fc)

	mc, err := adbh.Count(
		fmt.Sprintf(genderCountS, c.Name(), "male"),
	)
	if err != nil {
		t.Fatalf("error in running counting query %s", err)
	}
	assert.Equalf(mc, int64(15), "expect %d received %d", 15, mc)
}

func TestCountWithParams(t *testing.T) {
	c, err := setup(adbh, t)
	if err != nil {
		t.Fatalf("error in setup %s", err)
	}
	defer teardown(c, t)
	fc, err := adbh.CountWithParams(
		genderCount,
		map[string]interface{}{
			"@collection": c.Name(),
			"gender":      "female",
		},
	)
	if err != nil {
		t.Fatalf("error in running counting query %s", err)
	}
	assert := assert.New(t)
	assert.Equalf(fc, int64(15), "expect %d received %d", 15, fc)

	mc, err := adbh.CountWithParams(
		genderCount,
		map[string]interface{}{
			"@collection": c.Name(),
			"gender":      "male",
		},
	)
	if err != nil {
		t.Fatalf("error in running counting query %s", err)
	}
	assert.Equalf(mc, int64(15), "expect %d received %d", 15, mc)
}
