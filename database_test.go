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
	tra, err := newTestArangoFromEnv(true)
	if err != nil {
		log.Fatalf("unable to construct new TestArango instance %s", err)
	}
	dbh, err := tra.DB(tra.Database)
	if err != nil {
		log.Fatalf("unable to get database %s", err)
	}
	auser = tra.User
	apass = tra.Pass
	ahost = tra.Host
	aport = strconv.Itoa(tra.Port)
	adb = tra.Database
	adbh = dbh
	code := m.Run()
	if err := dbh.Drop(); err != nil {
		log.Fatalf("error in dropping database %s", err)
	}
	os.Exit(code)
}

func TestCount(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	fc, err := adbh.Count(fmt.Sprintf(genderQNoParam, conn.Name(), "female"))
	assert := assert.New(t)
	assert.NoErrorf(
		err,
		"expect no error from counting query, received error %s",
		err,
	)
	assert.Equalf(fc, int64(15), "expect %d received %d", 15, fc)
	mc, err := adbh.Count(fmt.Sprintf(genderQNoParam, conn.Name(), "male"))
	assert.NoErrorf(
		err,
		"expect no error from counting query, received error %s",
		err,
	)
	assert.Equalf(mc, int64(15), "expect %d received %d", 15, mc)
}

func TestCountWithParams(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	assert := assert.New(t)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			assert:     assert,
			collection: conn,
			gender:     g,
			count:      int64(15),
		})
	}
}

func TestCollection(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	_, err := adbh.Collection(RandomString(6, 8))
	assert := assert.New(t)
	assert.Error(
		err,
		"expect to return an error for an non-existent collection",
	)
	nc, err := adbh.Collection(conn.Name())
	assert.NoError(err, "not expect to return an error for existent collection")
	assert.Equalf(
		conn.Name(),
		nc.Name(),
		"expect %s, received %s",
		conn.Name(),
		nc.Name(),
	)
}

func TestCreateCollection(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	_, err := adbh.CreateCollection(conn.Name(), nil)
	assert := assert.New(t)
	assert.Error(err, "expect to return existing collection error")
	ncoll := RandomString(9, 11)
	nc, err := adbh.CreateCollection(ncoll, nil)
	assert.NoError(
		err,
		"not expect to return an error for non-existent collection",
	)
	assert.Equalf(
		ncoll,
		nc.Name(),
		"expect %s, received %s",
		"bogus",
		nc.Name(),
	)
}

func TestFindOrCreateCollection(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	ec, err := adbh.FindOrCreateCollection(c.Name(), nil)
	assert := assert.New(t)
	assert.NoError(err, "not expect to return an error for existent collection")
	assert.Equalf(
		c.Name(),
		ec.Name(),
		"expect %s, received %s",
		c.Name(),
		ec.Name(),
	)
	ncoll := RandomString(12, 15)
	nc, err := adbh.FindOrCreateCollection(ncoll, nil)
	assert.NoError(err, "not expect to return an error for existent collection")
	assert.Equalf(
		ncoll,
		nc.Name(),
		"expect %s, received %s",
		"bogus",
		nc.Name(),
	)
}

func TestEnsureGeoIndex(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	assert := assert.New(t)
	name := "value"
	index, b, err := adbh.EnsureGeoIndex(
		c.Name(),
		[]string{name},
		&driver.EnsureGeoIndexOptions{
			Name: name,
		},
	)
	assert.NoError(err, "should not return error for geo index method")
	assert.True(b, "should create geo index")
	assert.Exactly(
		index.Type(),
		driver.GeoIndex,
		"should return geo index type",
	)
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureGeoIndex(
		"wrong name",
		[]string{name},
		&driver.EnsureGeoIndexOptions{},
	)
	assert.Error(err, "should return error for wrong collection name")
}

func TestEnsureHashIndex(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	assert := assert.New(t)
	name := "entry_id"
	index, b, err := adbh.EnsureHashIndex(
		c.Name(),
		[]string{name},
		&driver.EnsureHashIndexOptions{
			Name: name,
		},
	)
	assert.NoError(err, "should not return error for hash index method")
	assert.True(b, "should create hash index")
	assert.Exactly(
		index.Type(),
		driver.HashIndex,
		"should return hash index type",
	)
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureHashIndex(
		"wrong name",
		[]string{name},
		&driver.EnsureHashIndexOptions{},
	)
	assert.Error(err, "should return error for wrong collection name")
}

func TestEnsurePersistentIndex(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	assert := assert.New(t)
	name := "entry_id"
	index, b, err := adbh.EnsurePersistentIndex(
		c.Name(),
		[]string{name},
		&driver.EnsurePersistentIndexOptions{
			Name: name,
		},
	)
	assert.NoError(err, "should not return error for index method")
	assert.True(b, "should create index")
	assert.Exactly(
		index.Type(),
		driver.PersistentIndex,
		"should return persistent index type",
	)
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsurePersistentIndex(
		"wrong name",
		[]string{name},
		&driver.EnsurePersistentIndexOptions{},
	)
	assert.Error(err, "should return error for wrong collection name")
}

func TestEnsureSkipListIndex(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	assert := assert.New(t)
	name := "created_at"
	index, b, err := adbh.EnsureSkipListIndex(
		c.Name(),
		[]string{name},
		&driver.EnsureSkipListIndexOptions{
			Name: name,
		},
	)
	assert.NoError(err, "should not return error for skip list index method")
	assert.True(b, "should create skip list index")
	assert.Exactly(
		index.Type(),
		driver.SkipListIndex,
		"should return skip list index type",
	)
	assert.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureSkipListIndex(
		"wrong name",
		[]string{name},
		&driver.EnsureSkipListIndexOptions{},
	)
	assert.Error(err, "should return error for wrong collection name")
}

func TestSearchRowsWithParams(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	frs, err := adbh.SearchRows(
		genderQ,
		map[string]interface{}{
			"@collection": conn.Name(),
			"gender":      "female",
		},
	)
	testSearchRs(t, frs, err)
	wrs, err := adbh.SearchRows(
		genderQ,
		map[string]interface{}{
			"@collection": conn.Name(),
			"gender":      "wakanda",
		},
	)
	testSearchRsNoRow(t, wrs, err)
}

func TestSearchRows(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	frs, err := adbh.Search(fmt.Sprintf(genderQNoParam, c.Name(), "female"))
	testSearchRs(t, frs, err)
	wrs, err := adbh.Search(fmt.Sprintf(genderQNoParam, c.Name(), "wakanda"))
	testSearchRsNoRow(t, wrs, err)
}

func TestDo(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
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
	assert.NoErrorf(
		err,
		"expect no error from insert query, received error %s",
		err,
	)
}

func TestGetRow(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	row, err := adbh.GetRow(
		userQ,
		map[string]interface{}{
			"@collection": conn.Name(),
			"first":       "Mickie",
			"last":        "Menchaca",
		},
	)
	assert := assert.New(t)
	assert.NoErrorf(
		err,
		"expect no error from search query, received error %s",
		err,
	)
	assert.False(row.IsEmpty(), "expect result to be not empty")
	var u testUserDb
	err = row.Read(&u)
	assert.NoError(err, "expect no error from reading the data")
	assert.Equal(u.Gender, "female", "expect gender to be female")
	assert.Equal(
		u.Contact.Address.City,
		"Beachwood",
		"should match city Beachwood",
	)
	assert.Equal(u.Contact.Region, "732", "should match region 732")
	erow, err := adbh.GetRow(
		userQ,
		map[string]interface{}{
			"@collection": conn.Name(),
			"first":       "Pantu",
			"last":        "Boka",
		},
	)
	assert.NoErrorf(
		err,
		"expect no error from row query, received error %s",
		err,
	)
	assert.True(erow.IsEmpty(), "expect empty resultset")
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	assert := assert.New(t)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			assert:     assert,
			collection: conn,
			gender:     g,
			count:      int64(15),
		})
	}
	err := adbh.Truncate(conn.Name())
	assert.NoErrorf(
		err,
		"expect no error from truncation, received error %s",
		err,
	)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			assert:     assert,
			collection: conn,
			gender:     g,
			count:      int64(0),
		})
	}
}

func testGenderCount(args *genderCountParams) {
	gcp, err := adbh.CountWithParams(
		genderQ,
		map[string]interface{}{
			"@collection": args.collection.Name(),
			"gender":      args.gender,
		},
	)
	args.assert.NoErrorf(
		err,
		"expect no error from counting query, received error %s",
		err,
	)
	args.assert.Equalf(
		gcp,
		args.count,
		"expect %d received %d",
		args.count,
		gcp,
	)
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

func testSearchRs(t *testing.T, rs *Resultset, err error) {
	t.Helper()
	assert := assert.New(t)
	assert.NoErrorf(
		err,
		"expect no error from search query, received error %s",
		err,
	)
	assert.False(rs.IsEmpty(), "expect resultset to be not empty")
	testAllRows(rs, assert, 15)
}

func testSearchRsNoRow(t *testing.T, rs *Resultset, err error) {
	t.Helper()
	assert := assert.New(t)
	assert.NoErrorf(
		err,
		"expect no error from search query, received error %s",
		err,
	)
	assert.True(rs.IsEmpty(), "expect empty resultset")
	
	// Test for the fix: calling Scan() and Close() on empty resultsets shouldn't panic
	assert.False(rs.Scan(), "scan on empty resultset should return false")
	assert.NoError(rs.Close(), "close on empty resultset should not error")
}
