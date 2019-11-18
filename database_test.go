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
	testSearchRs(frs, err, t)
	wrs, err := adbh.SearchRows(
		genderQ,
		map[string]interface{}{
			"@collection": c.Name(),
			"gender":      "wakanda",
		},
	)
	testSearchRsNoRow(wrs, err, t)
}

func TestSearchRows(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	frs, err := adbh.Search(fmt.Sprintf(genderQNoParam, c.Name(), "female"))
	testSearchRs(frs, err, t)
	wrs, err := adbh.Search(fmt.Sprintf(genderQNoParam, c.Name(), "wakanda"))
	testSearchRsNoRow(wrs, err, t)
}

func TestDo(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	err := adbh.Do(
		fmt.Sprintf(userIns, c.Name()),
		map[string]interface{}{
			"first":  "Chitkini",
			"last":   "Dey",
			"gender": "male",
			"region": "gram",
			"city":   "porgona",
			"state":  "wb",
			"zip":    "48943",
		},
	)
	assert := assert.New(t)
	assert.NoErrorf(err, "expect no error from insert query, received error %s", err)
}

func TestGetRow(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	r, err := adbh.GetRow(
		userQ,
		map[string]interface{}{
			"@collection": c.Name(),
			"first":       "Mickie",
			"last":        "Menchaca",
		},
	)
	assert := assert.New(t)
	assert.NoErrorf(err, "expect no error from search query, received error %s", err)
	assert.False(r.IsEmpty(), "expect result to be not empty")
	var u testUserDb
	err = r.Read(&u)
	assert.NoError(err, "expect no error from reading the data")
	assert.Equal(u.Gender, "female", "expect gender to be female")
	assert.Equal(u.Contact.Address.City, "Beachwood", "should match city Beachwood")
	assert.Equal(u.Contact.Region, "732", "should match region 732")
	er, err := adbh.GetRow(
		userQ,
		map[string]interface{}{
			"@collection": c.Name(),
			"first":       "Pantu",
			"last":        "Boka",
		},
	)
	assert.NoErrorf(err, "expect no error from row query, received error %s", err)
	assert.True(er.IsEmpty(), "expect empty resultset")
}

func testAllRows(rs *Resultset, assert *assert.Assertions, count int) {
	for i := 0; i < count; i++ {
		assert.True(rs.Scan(), "expect scanning of record")
		var u testUserDb
		err := rs.Read(&u)
		assert.NoError(err, "expect no error from reading the data")
		assert.Equal(u.Gender, "female", "expect gender to be female")
	}
}

func testSearchRs(rs *Resultset, err error, t *testing.T) {
	assert := assert.New(t)
	assert.NoErrorf(err, "expect no error from search query, received error %s", err)
	assert.False(rs.IsEmpty(), "expect resultset to be not empty")
	testAllRows(rs, assert, 15)
}

func testSearchRsNoRow(rs *Resultset, err error, t *testing.T) {
	assert := assert.New(t)
	assert.NoErrorf(err, "expect no error from search query, received error %s", err)
	assert.True(rs.IsEmpty(), "expect empty resultset")
}
