package arangomanager

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var ahost, aport, auser, apass, adb string
var adbh *Database

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

func TestCount(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	fc, err := adbh.Count(fmt.Sprintf(genderQNoParam, c.Name(), "female"))
	assert := assert.New(t)
	assert.NoErrorf(err, "expect no error from counting query, received error %s", err)
	assert.Equalf(fc, int64(15), "expect %d received %d", 15, fc)
	mc, err := adbh.Count(fmt.Sprintf(genderQNoParam, c.Name(), "male"))
	assert.NoErrorf(err, "expect no error from counting query, received error %s", err)
	assert.Equalf(mc, int64(15), "expect %d received %d", 15, mc)
}

func TestCountWithParams(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	fc, err := adbh.CountWithParams(
		genderQ,
		map[string]interface{}{
			"@collection": c.Name(),
			"gender":      "female",
		},
	)
	assert := assert.New(t)
	assert.NoErrorf(err, "expect no error from counting query, received error %s", err)
	assert.Equalf(fc, int64(15), "expect %d received %d", 15, fc)
	mc, err := adbh.CountWithParams(
		genderQ,
		map[string]interface{}{
			"@collection": c.Name(),
			"gender":      "male",
		},
	)
	assert.NoErrorf(err, "expect no error, received error %s", err)
	assert.Equalf(mc, int64(15), "expect %d received %d", 15, mc)
}

func TestCollection(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	_, err := adbh.Collection(randomString(6, 8))
	assert := assert.New(t)
	assert.Error(err, "expect to return an error for an non-existent collection")
	nc, err := adbh.Collection(c.Name())
	assert.NoError(err, "not expect to return an error for existent collection")
	assert.Equalf(c.Name(), nc.Name(), "expect %s, received %s", c.Name(), nc.Name())
}

func TestCreateCollection(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	_, err := adbh.CreateCollection(c.Name(), nil)
	assert := assert.New(t)
	assert.Error(err, "expect to return existing collection error")
	ncoll := randomString(9, 11)
	nc, err := adbh.CreateCollection(ncoll, nil)
	assert.NoError(err, "not expect to return an error for non-existent collection")
	assert.Equalf(ncoll, nc.Name(), "expect %s, received %s", "bogus", nc.Name())
}

func TestFindOrCreateCollection(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	ec, err := adbh.FindOrCreateCollection(c.Name(), nil)
	assert := assert.New(t)
	assert.NoError(err, "not expect to return an error for existent collection")
	assert.Equalf(c.Name(), ec.Name(), "expect %s, received %s", c.Name(), ec.Name())
	ncoll := randomString(12, 15)
	nc, err := adbh.FindOrCreateCollection(ncoll, nil)
	assert.NoError(err, "not expect to return an error for existent collection")
	assert.Equalf(ncoll, nc.Name(), "expect %s, received %s", "bogus", nc.Name())
}

func TestSearchRowsWithParams(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	frs, err := adbh.SearchRows(
		genderQ,
		map[string]interface{}{
			"@collection": c.Name(),
			"gender":      "female",
		},
	)
	assert := assert.New(t)
	assert.NoErrorf(err, "expect no error from search query, received error %s", err)
	assert.False(frs.IsEmpty(), "expect resultset to be not empty")
	for i := 0; i < 15; i++ {
		assert.True(frs.Scan(), "expect scanning of record")
		var u testUserDb
		err := frs.Read(&u)
		assert.NoError(err, "expect no error from reading the data")
		assert.Equal(u.Gender, "female", "expect gender to be female")
	}
	wrs, err := adbh.SearchRows(
		genderQ,
		map[string]interface{}{
			"@collection": c.Name(),
			"gender":      "wakanda",
		},
	)
	assert.NoErrorf(err, "expect no error from search query, received error %s", err)
	assert.True(wrs.IsEmpty(), "expect emtpy resultset")
}

func TestSearchRows(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	frs, err := adbh.Search(fmt.Sprintf(genderQNoParam, c.Name(), "female"))
	assert := assert.New(t)
	assert.NoErrorf(err, "expect no error from search query, received error %s", err)
	assert.False(frs.IsEmpty(), "expect resultset to be not empty")
	for i := 0; i < 15; i++ {
		assert.True(frs.Scan(), "expect scanning of record")
		var u testUserDb
		err := frs.Read(&u)
		assert.NoError(err, "expect no error from reading the data")
		assert.Equal(u.Gender, "female", "expect gender to be female")
	}
	wrs, err := adbh.Search(fmt.Sprintf(genderQNoParam, c.Name(), "wakanda"))
	assert.NoErrorf(err, "expect no error from search query, received error %s", err)
	assert.True(wrs.IsEmpty(), "expect emtpy resultset")
}
