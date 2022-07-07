package query

import (
	"fmt"
	"log"
	"testing"

	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/arangomanager"
	"github.com/dictyBase/arangomanager/testarango"
	"github.com/stretchr/testify/require"
)

const (
	minLen = 20
	maxLen = 30
)

// mapping of filters to database fields.
var fmap = map[string]string{
	"created_at": "created_at",
	"sport":      "sports",
	"email":      "email",
	"label":      "label",
}

var qmap = map[string]string{
	"created_at": "foo.created_at",
	"sport":      "bar.game",
	"email":      "fizz.identifier",
	"label":      "v.label",
}

var gta *testarango.TestArango

type testFilterFn func(assert *require.Assertions, dbh *arangomanager.Database)

func TestParseFilterString(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	fls, err := ParseFilterString("sport===football;email===mahomes@gmail.com")
	assert.NoError(err, "should not return any parse error")
	assert.Len(fls, 2, "should match length of two items in filter array")
	assert.Equal(fls[0].Value, "football", "should match the sport query")
	assert.Equal(fls[1].Value, "mahomes@gmail.com", "should match the email query")
	assert.Equal(fls[0].Field, "sport", "should match field sport")
	assert.Equal(fls[1].Field, "email", "should match fieldi email")
	assert.Equal(fls[0].Operator, "===", "should match equal operator")
	assert.Equal(fls[1].Operator, "===", "should match equal operator")
	assert.Equal(fls[0].Logic, ";", "should have parsed colon logic")
	assert.Empty(fls[1].Logic, "should have empty logic value")

	fls2, err := ParseFilterString("ontology!~dicty annotation;tag=~logicx")
	assert.NoError(err, "should not return any parse error")
	assert.Len(fls2, 2, "should have two items in filter array")
	assert.Equal(fls2[0].Value, "dicty annotation", "should match ontology query")
	assert.Equal(fls2[1].Value, "logicx", "should match tag query")
	assert.Equal(fls2[0].Field, "ontology", "should match field ontology")
	assert.Equal(fls2[1].Field, "tag", "should match field annotation")
	assert.Equal(fls2[0].Operator, "!~", "should match regexp match operator")
	assert.Equal(fls2[1].Operator, "=~", "should match regexp negation operator")
	assert.Equal(fls2[0].Logic, ";", "should have parsed colon logic")
	assert.Empty(fls2[1].Logic, "should have empty logic value")

	b, err := ParseFilterString("xyz")
	assert.NoError(err, "should not return any parse error")
	assert.Len(b, 0, "should have empty slice since regex doesn't match string")
}

func TestGenQualifiedAQLFilterStatement(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	ta, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err, "should not produce any error from testarango constructor")
	gta = ta
	dbh, err := ta.DB(ta.Database)
	assert.NoError(err, "should not produce any database error")
	crnd := testarango.RandomString(6, 10)
	_, err = dbh.CreateCollection(crnd, &driver.CreateCollectionOptions{})
	if err != nil {
		e := dbh.Drop()
		assert.NoError(e, "should not produce any error from database removal")
	}
	defer func() {
		err := dbh.Drop()
		assert.NoError(err, "should not produce any error from database removal")
	}()
	// test string equals with OR operator
	f, err := ParseFilterString("email===mahomes@gmail.com,email===brees@gmail.com")
	assert.NoError(err, "should not return any parsing error")
	nqa, err := GenQualifiedAQLFilterStatement(qmap, f)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Equal(
		nqa,
		"FILTER fizz.identifier == 'mahomes@gmail.com'\n OR fizz.identifier == 'brees@gmail.com'",
		"should match filter statement",
	)
	err = dbh.ValidateQ(genFullQualifiedStmt(nqa, "fizz", crnd))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item substring for quotes
	qf, err := ParseFilterString("label=~GWDI")
	assert.NoError(err, "should not return any parsing error")
	qs, err := GenQualifiedAQLFilterStatement(qmap, qf)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Equal(qs, "FILTER v.label =~ 'GWDI'", "should contain GWDI substring")
	err = dbh.ValidateQ(genFullQualifiedStmt(qs, "v", crnd))
	assert.NoError(err, "should not have any invalid AQL query")
	// test date equals
	df, err := ParseFilterString("created_at$==2019,created_at$==2018")
	assert.NoError(err, "should not return any parsing error")
	dfl, err := GenQualifiedAQLFilterStatement(qmap, df)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Equal(dfl, "FILTER foo.created_at == DATE_ISO8601('2019')\n OR foo.created_at == DATE_ISO8601('2018')")
	err = dbh.ValidateQ(genFullQualifiedStmt(dfl, "foo", crnd))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item in array equals
	af, err := ParseFilterString("sport@==basketball")
	assert.NoError(err, "should not return any parsing error")
	afn, err := GenQualifiedAQLFilterStatement(qmap, af)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(afn, "LET", "should contain LET term, indicating array item")
	assert.Contains(
		afn,
		"FILTER 'basketball' IN bar.game[*]",
		"should contain an array containing statement",
	)
	err = dbh.ValidateQ(genFullQualifiedStmt(afn, "bar", crnd))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item substring in array
	af2, err := ParseFilterString("sport@=~basket")
	assert.NoError(err, "should not return any parsing error")
	an2, err := GenQualifiedAQLFilterStatement(qmap, af2)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(
		an2,
		"FILTER CONTAINS(x, LOWER('basket'))",
		"should contain FILTER CONTAINS statement, indicating array item substring",
	)
	assert.Contains(an2, "LIMIT 1", "should match limit of one")
	err = dbh.ValidateQ(genFullQualifiedStmt(an2, "bar", crnd))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item in array not equals
	bf, err := ParseFilterString("sport@!=banana,sport@!=apple")
	assert.NoError(err, "should not return any parsing error")
	bns, err := GenQualifiedAQLFilterStatement(qmap, bf)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(bns, "NOT IN", "should contain NOT IN statement, indicating item is not in array")
	assert.Contains(bns, "OR", "should contain OR term")
	assert.Contains(
		bns,
		"FILTER 'banana' NOT IN bar.game[*]",
		"should contain filter with NOT IN operator with collection and column name",
	)
	assert.Contains(
		bns,
		"FILTER 'apple' NOT IN bar.game[*]",
		"should contain filter with NOT IN operator with collection and column name",
	)
	err = dbh.ValidateQ(genFullQualifiedStmt(bns, "bar", crnd))
	assert.NoError(err, "should not have any invalid AQL query")
}

func TestGenAQLFilterStatement(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	ta, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err, "should not produce any error from testarango constructor")
	gta = ta
	dbh, err := ta.DB(ta.Database)
	assert.NoError(err, "should not produce any database error")
	cstr := testarango.RandomString(20, 30)
	_, err = dbh.CreateCollection(cstr, &driver.CreateCollectionOptions{})
	if err != nil {
		e := dbh.Drop()
		assert.NoError(e, "should not produce any error from database removal")
	}
	defer func() {
		e := dbh.Drop()
		if e != nil {
			log.Fatalf("could not remove database %s", e)
		}
	}()
	for _, tfn := range []testFilterFn{
		testSubstringFilter, testEqualFilter,
		testOperatorFilter, testDateFilter,
		testArrayFilter,
	} {
		tfn(assert, dbh)
	}
}

func testArrayFilter(assert *require.Assertions, dbh *arangomanager.Database) {
	cstr := testarango.RandomString(minLen, maxLen)
	as, err := ParseFilterString("sport@==basketball")
	assert.NoError(err, "should not have any error from parsing string")
	afn, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: as, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(afn, "LET", "should contain LET term, indicating array item")
	assert.Contains(
		afn,
		"FILTER 'basketball' IN doc.sports[*]",
		"should contain FILTER and IN term",
	)
	err = dbh.ValidateQ(genFullStmt(afn, cstr))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item substring in array
	a, err := ParseFilterString("sport@=~basket")
	assert.NoError(err, "should not have any error from parsing string")
	afl, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: a, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(afn, "LET", "should contain LET term, indicating array item")
	assert.Contains(
		afl,
		"FILTER CONTAINS(x, LOWER('basket')) ",
		"should contain FILTER CONTAINS statement, indicating array item substring",
	)
	err = dbh.ValidateQ(genFullStmt(afl, cstr))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item in array with OR logic
	b, err := ParseFilterString("sport@!=banana,sport@==apple")
	assert.NoError(err, "should not have any error from parsing string")
	bfa, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: b, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(
		bfa,
		"FILTER 'apple' IN doc.sports[*]",
		"should contain IN statement",
	)
	assert.Contains(
		bfa,
		"FILTER 'banana' NOT IN doc.sports[*]",
		"should contain NOT IN statement",
	)
	assert.Contains(bfa, "OR", "should contain OR term")
	err = dbh.ValidateQ(genFullStmt(bfa, cstr))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item in array with AND logic
	b2, err := ParseFilterString("sport@=~banana;sport@==apple")
	assert.NoError(err, "should not have any error from parsing string")
	bf2, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: b2, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(
		bf2,
		"FILTER 'apple' IN doc.sports[*]",
		"should contain IN statement",
	)
	assert.Contains(
		bf2,
		"FILTER CONTAINS(x, LOWER('banana'))",
		"should contain CONTAINS statement",
	)
	assert.Contains(bf2, "AND", "should contain AND logic")
	err = dbh.ValidateQ(genFullStmt(bfa, cstr))
	assert.NoError(err, "should not have any invalid AQL query")
}

func testDateFilter(assert *require.Assertions, dbh *arangomanager.Database) {
	cstr := testarango.RandomString(minLen, maxLen)
	ds, err := ParseFilterString("created_at$==2019,created_at$>2018")
	assert.NoError(err, "should not have any error from parsing string")
	dfl, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: ds, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(dfl, "doc.created_at == DATE_ISO8601('2019')", "should contain DATE_ISO8601 term")
	assert.Contains(dfl, "doc.created_at > DATE_ISO8601('2018')", "should contain DATE_ISO8601 term")
	assert.Contains(dfl, "OR", "should contain OR term")
	err = dbh.ValidateQ(genFullStmt(dfl, cstr))
	assert.NoError(err, "should not have any invalid AQL query")
	ds2, err := ParseFilterString("created_at$<2019;created_at$<=2018;created_at$>=2020")
	assert.NoError(err, "should not have any error from parsing string")
	dn2, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: ds2, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(dn2, "FILTER doc.created_at < DATE_ISO8601('2019')", "should contain DATE_ISO8601 term")
	assert.Contains(dn2, "doc.created_at <= DATE_ISO8601('2018')", "should contain DATE_ISO8601 term")
	assert.Contains(dn2, "doc.created_at >= DATE_ISO8601('2020')", "should contain DATE_ISO8601 term")
	assert.Contains(dn2, "AND", "should contain AND term")
	err = dbh.ValidateQ(genFullStmt(dfl, cstr))
	assert.NoError(err, "should not have any invalid AQL query")
}

func testSubstringFilter(assert *require.Assertions, dbh *arangomanager.Database) {
	cstr := testarango.RandomString(minLen, maxLen)
	qf, err := ParseFilterString("label=~GWDI")
	assert.NoError(err, "should not return any parsing error")
	qs, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: qf, Doc: "doc"})
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(qs, "FILTER", "should contain FILTER term")
	assert.Contains(qs, "doc.label =~ 'GWDI'", "should contain GWDI substring")
	err = dbh.ValidateQ(genFullStmt(qs, cstr))
	assert.NoError(err, "should not have any invalid AQL query")
	// substring match with AND logic
	qf2, err := ParseFilterString("label=~GWDI;email===brady@gmail.com")
	assert.NoError(err, "should not return any parsing error")
	qs2, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: qf2, Doc: "doc"})
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(qs2, "FILTER", "should contain FILTER term")
	assert.Contains(qs2, "doc.label =~ 'GWDI'", "should contain GWDI substring")
	assert.Contains(qs2, "doc.email == 'brady@gmail.com'", "should contain proper == statement")
	err = dbh.ValidateQ(genFullStmt(qs2, cstr))
	assert.NoError(err, "should not have any invalid AQL query")
}

func testOperatorFilter(assert *require.Assertions, dbh *arangomanager.Database) {
	s2, err := ParseFilterString("email===mahomes@gmail.com;email===brees@gmail.com")
	assert.NoError(err, "should not have any error from parsing string")
	na2, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: s2, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(na2, "FILTER", "should contain FILTER term")
	assert.Contains(na2, "doc.email == 'mahomes@gmail.com'", "should contain proper == statement")
	assert.Contains(na2, "doc.email == 'brees@gmail.com'", "should contain proper == statement")
	assert.Contains(na2, "AND", "should contain AND term")
	err = dbh.ValidateQ(genFullStmt(na2, testarango.RandomString(minLen, maxLen)))
	assert.NoError(err, "should not have any invalid AQL query")
}

func testEqualFilter(assert *require.Assertions, dbh *arangomanager.Database) {
	s, err := ParseFilterString("email===mahomes@gmail.com,email===brees@gmail.com")
	assert.NoError(err, "should not have any error from parsing string")
	naf, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: s, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(naf, "FILTER", "should contain FILTER term")
	assert.Contains(naf, "doc.email == 'mahomes@gmail.com'", "should contain proper == statement")
	assert.Contains(naf, "doc.email == 'brees@gmail.com'", "should contain proper == statement")
	assert.Contains(naf, "OR", "should contain OR term")
	err = dbh.ValidateQ(genFullStmt(naf, testarango.RandomString(minLen, maxLen)))
	assert.NoError(err, "should not have any invalid AQL query")
}

func genFullQualifiedStmt(filter, name, coll string) string {
	return fmt.Sprintf(
		`
		FOR %s in %s
			%s
			RETURN %s
		`, name, coll, filter, name,
	)
}

func genFullStmt(filter, coll string) string {
	return fmt.Sprintf(
		`
		FOR doc in %s
			%s
			RETURN doc
		`,
		coll, filter,
	)
}
