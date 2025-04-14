package arangomanager

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	driver "github.com/arangodb/go-driver"
	"github.com/stretchr/testify/require"
)

var (
	ahost, aport, auser, apass, adb string
	adbh                            *Database
)

type genderCountParams struct {
	require    *require.Assertions
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
	require := require.New(t)
	require.NoErrorf(
		err,
		"expect no error from counting query, received error %s",
		err,
	)
	require.Equalf(fc, int64(15), "expect %d received %d", 15, fc)
	mc, err := adbh.Count(fmt.Sprintf(genderQNoParam, conn.Name(), "male"))
	require.NoErrorf(
		err,
		"expect no error from counting query, received error %s",
		err,
	)
	require.Equalf(mc, int64(15), "expect %d received %d", 15, mc)
}

func TestCountWithParams(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	require := require.New(t)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			require:    require,
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
	require := require.New(t)
	require.Error(
		err,
		"expect to return an error for an non-existent collection",
	)
	nc, err := adbh.Collection(conn.Name())
	require.NoError(
		err,
		"not expect to return an error for existent collection",
	)
	require.Equalf(
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
	require := require.New(t)
	require.Error(err, "expect to return existing collection error")
	ncoll := RandomString(9, 11)
	nc, err := adbh.CreateCollection(ncoll, nil)
	require.NoError(
		err,
		"not expect to return an error for non-existent collection",
	)
	require.Equalf(
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
	require := require.New(t)
	require.NoError(
		err,
		"not expect to return an error for existent collection",
	)
	require.Equalf(
		c.Name(),
		ec.Name(),
		"expect %s, received %s",
		c.Name(),
		ec.Name(),
	)
	ncoll := RandomString(12, 15)
	nc, err := adbh.FindOrCreateCollection(ncoll, nil)
	require.NoError(
		err,
		"not expect to return an error for existent collection",
	)
	require.Equalf(
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
	require := require.New(t)
	name := "value"
	index, b, err := adbh.EnsureGeoIndex(
		c.Name(),
		[]string{name},
		&driver.EnsureGeoIndexOptions{
			Name: name,
		},
	)
	require.NoError(err, "should not return error for geo index method")
	require.True(b, "should create geo index")
	require.Exactly(
		index.Type(),
		driver.GeoIndex,
		"should return geo index type",
	)
	require.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureGeoIndex(
		"wrong name",
		[]string{name},
		&driver.EnsureGeoIndexOptions{},
	)
	require.Error(err, "should return error for wrong collection name")
}

func TestEnsureHashIndex(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	require := require.New(t)
	name := "entry_id"
	index, b, err := adbh.EnsureHashIndex(
		c.Name(),
		[]string{name},
		&driver.EnsureHashIndexOptions{
			Name: name,
		},
	)
	require.NoError(err, "should not return error for hash index method")
	require.True(b, "should create hash index")
	require.Exactly(
		index.Type(),
		driver.HashIndex,
		"should return hash index type",
	)
	require.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureHashIndex(
		"wrong name",
		[]string{name},
		&driver.EnsureHashIndexOptions{},
	)
	require.Error(err, "should return error for wrong collection name")
}

func TestEnsurePersistentIndex(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	require := require.New(t)
	name := "entry_id"
	index, b, err := adbh.EnsurePersistentIndex(
		c.Name(),
		[]string{name},
		&driver.EnsurePersistentIndexOptions{
			Name: name,
		},
	)
	require.NoError(err, "should not return error for index method")
	require.True(b, "should create index")
	require.Exactly(
		index.Type(),
		driver.PersistentIndex,
		"should return persistent index type",
	)
	require.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsurePersistentIndex(
		"wrong name",
		[]string{name},
		&driver.EnsurePersistentIndexOptions{},
	)
	require.Error(err, "should return error for wrong collection name")
}

func TestEnsureSkipListIndex(t *testing.T) {
	t.Parallel()
	c := setup(t, adbh)
	defer teardown(t, c)
	require := require.New(t)
	name := "created_at"
	index, b, err := adbh.EnsureSkipListIndex(
		c.Name(),
		[]string{name},
		&driver.EnsureSkipListIndexOptions{
			Name: name,
		},
	)
	require.NoError(err, "should not return error for skip list index method")
	require.True(b, "should create skip list index")
	require.Exactly(
		index.Type(),
		driver.SkipListIndex,
		"should return skip list index type",
	)
	require.Exactly(index.UserName(), name, "should match provided name option")
	_, _, err = adbh.EnsureSkipListIndex(
		"wrong name",
		[]string{name},
		&driver.EnsureSkipListIndexOptions{},
	)
	require.Error(err, "should return error for wrong collection name")
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
	require := require.New(t)
	require.NoErrorf(
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
	require := require.New(t)
	require.NoErrorf(
		err,
		"expect no error from search query, received error %s",
		err,
	)
	require.False(row.IsEmpty(), "expect result to be not empty")
	var u testUserDb
	err = row.Read(&u)
	require.NoError(err, "expect no error from reading the data")
	require.Equal(u.Gender, "female", "expect gender to be female")
	require.Equal(
		u.Contact.Address.City,
		"Beachwood",
		"should match city Beachwood",
	)
	require.Equal(u.Contact.Region, "732", "should match region 732")
	erow, err := adbh.GetRow(
		userQ,
		map[string]interface{}{
			"@collection": conn.Name(),
			"first":       "Pantu",
			"last":        "Boka",
		},
	)
	require.NoErrorf(
		err,
		"expect no error from row query, received error %s",
		err,
	)
	require.True(erow.IsEmpty(), "expect empty resultset")
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	conn := setup(t, adbh)
	defer teardown(t, conn)
	require := require.New(t)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			require:    require,
			collection: conn,
			gender:     g,
			count:      int64(15),
		})
	}
	err := adbh.Truncate(conn.Name())
	require.NoErrorf(
		err,
		"expect no error from truncation, received error %s",
		err,
	)
	for _, g := range []string{"male", "female"} {
		testGenderCount(&genderCountParams{
			require:    require,
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
	args.require.NoErrorf(
		err,
		"expect no error from counting query, received error %s",
		err,
	)
	args.require.Equalf(
		gcp,
		args.count,
		"expect %d received %d",
		args.count,
		gcp,
	)
}

func testAllRows(rs *Resultset, require *require.Assertions, count int) {
	for i := 0; i < count; i++ {
		require.True(rs.Scan(), "expect scanning of record")
		var u testUserDb
		err := rs.Read(&u)
		require.NoError(err, "expect no error from reading the data")
		require.Equal(u.Gender, "female", "expect gender to be female")
	}
}

func testSearchRs(t *testing.T, rs *Resultset, err error) {
	t.Helper()
	require := require.New(t)
	require.NoErrorf(
		err,
		"expect no error from search query, received error %s",
		err,
	)
	require.False(rs.IsEmpty(), "expect resultset to be not empty")
	testAllRows(rs, require, 15)
	require.False(rs.Scan(), "should be false")
	require.NoError(rs.Close(), "should not return error")
}

func testSearchRsNoRow(t *testing.T, rs *Resultset, err error) {
	t.Helper()
	require := require.New(t)
	require.NoErrorf(
		err,
		"expect no error from search query, received error %s",
		err,
	)
	require.True(rs.IsEmpty(), "expect empty resultset")
	// Test for the fix: calling Scan() and Close() on empty resultsets shouldn't panic
	require.False(rs.Scan(), "scan on empty resultset should return false")
	require.NoError(rs.Close(), "close on empty resultset should not error")
}
