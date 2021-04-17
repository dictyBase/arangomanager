package arangomanager

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	driver "github.com/arangodb/go-driver"
	"github.com/stretchr/testify/assert"
)

var ahost, aport, auser, apass, adb string
var adbh *Database

type genderCountParams struct {
	assert     *assert.Assertions
	collection driver.Collection
	gender     string
	count      int64
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
	assert := assert.New(t)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			assert:     assert,
			collection: c,
			gender:     g,
			count:      int64(15),
		})
	}
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

func TestEnsureFullTextIndex(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	assert := assert.New(t)
	name := "group"
	index, b, err := adbh.EnsureFullTextIndex(c.Name(), []string{name}, &driver.EnsureFullTextIndexOptions{
		Name: name,
	})
	assert.NoError(err, "should not return error for full text index method")
	assert.True(b, "should create full text index")
	assert.Exactly(index.Type(), driver.FullTextIndex, "should return full text index type")
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureFullTextIndex("wrong name", []string{name}, &driver.EnsureFullTextIndexOptions{})
	assert.Error(err, "should return error for wrong collection name")
}

func TestEnsureGeoIndex(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	assert := assert.New(t)
	name := "value"
	index, b, err := adbh.EnsureGeoIndex(c.Name(), []string{name}, &driver.EnsureGeoIndexOptions{
		Name: name,
	})
	assert.NoError(err, "should not return error for geo index method")
	assert.True(b, "should create geo index")
	assert.Exactly(index.Type(), driver.GeoIndex, "should return geo index type")
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureGeoIndex("wrong name", []string{name}, &driver.EnsureGeoIndexOptions{})
	assert.Error(err, "should return error for wrong collection name")
}

func TestEnsureHashIndex(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	assert := assert.New(t)
	name := "entry_id"
	index, b, err := adbh.EnsureHashIndex(c.Name(), []string{name}, &driver.EnsureHashIndexOptions{
		Name: name,
	})
	assert.NoError(err, "should not return error for hash index method")
	assert.True(b, "should create hash index")
	assert.Exactly(index.Type(), driver.HashIndex, "should return hash index type")
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureHashIndex("wrong name", []string{name}, &driver.EnsureHashIndexOptions{})
	assert.Error(err, "should return error for wrong collection name")
}

func TestEnsurePersistentIndex(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	assert := assert.New(t)
	name := "entry_id"
	index, b, err := adbh.EnsurePersistentIndex(c.Name(), []string{name}, &driver.EnsurePersistentIndexOptions{
		Name: name,
	})
	assert.NoError(err, "should not return error for index method")
	assert.True(b, "should create index")
	assert.Exactly(index.Type(), driver.PersistentIndex, "should return persistent index type")
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsurePersistentIndex("wrong name", []string{name}, &driver.EnsurePersistentIndexOptions{})
	assert.Error(err, "should return error for wrong collection name")
}

func TestEnsureSkipListIndex(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	assert := assert.New(t)
	name := "created_at"
	index, b, err := adbh.EnsureSkipListIndex(c.Name(), []string{name}, &driver.EnsureSkipListIndexOptions{
		Name: name,
	})
	assert.NoError(err, "should not return error for skip list index method")
	assert.True(b, "should create skip list index")
	assert.Exactly(index.Type(), driver.SkipListIndex, "should return skip list index type")
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureSkipListIndex("wrong name", []string{name}, &driver.EnsureSkipListIndexOptions{})
	assert.Error(err, "should return error for wrong collection name")
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

func TestTruncate(t *testing.T) {
	t.Parallel()
	c := setup(adbh, t)
	defer teardown(c, t)
	assert := assert.New(t)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			assert:     assert,
			collection: c,
			gender:     g,
			count:      int64(15),
		})
	}
	err := adbh.Truncate(c.Name())
	assert.NoErrorf(err, "expect no error from truncation, received error %s", err)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			assert:     assert,
			collection: c,
			gender:     g,
			count:      int64(0),
		})
	}
}

func testGenderCount(args *genderCountParams) {
	gc, err := adbh.CountWithParams(
		genderQ,
		map[string]interface{}{
			"@collection": args.collection.Name(),
			"gender":      args.gender,
		},
	)
	args.assert.NoErrorf(err, "expect no error from counting query, received error %s", err)
	args.assert.Equalf(gc, args.count, "expect %d received %d", args.count, gc)
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
