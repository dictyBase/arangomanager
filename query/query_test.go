package query

import (
	"fmt"
	"log"
	"testing"

	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/arangomanager/testarango"

	"github.com/stretchr/testify/require"
)

// mapping of filters to database fields
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

func TestParseFilterString(t *testing.T) {
	t.Parallel()
	assert := require.New(t)
	s, err := ParseFilterString("sport===football;email===mahomes@gmail.com")
	assert.NoError(err, "should not return any parse error")
	assert.Len(s, 2, "should match length of two items in filter array")
	assert.Equal(s[0].Value, "football", "should match the sport query")
	assert.Equal(s[1].Value, "mahomes@gmail.com", "should match the email query")
	assert.Equal(s[0].Field, "sport", "should match field sport")
	assert.Equal(s[1].Field, "email", "should match fieldi email")
	assert.Equal(s[0].Operator, "===", "should match equal operator")
	assert.Equal(s[1].Operator, "===", "should match equal operator")
	assert.Equal(s[0].Logic, ";", "should have parsed colon logic")
	assert.Empty(s[1].Logic, "should have empty logic value")

	b2, err := ParseFilterString("ontology!~dicty annotation;tag=~logicx")
	assert.NoError(err, "should not return any parse error")
	assert.Len(b2, 2, "should have two items in filter array")
	assert.Equal(b2[0].Value, "dicty annotation", "should match ontology query")
	assert.Equal(b2[1].Value, "logicx", "should match tag query")
	assert.Equal(b2[0].Field, "ontology", "should match field ontology")
	assert.Equal(b2[1].Field, "tag", "should match field annotation")
	assert.Equal(b2[0].Operator, "!~", "should match regexp match operator")
	assert.Equal(b2[1].Operator, "=~", "should match regexp negation operator")
	assert.Equal(b2[0].Logic, ";", "should have parsed colon logic")
	assert.Empty(b2[1].Logic, "should have empty logic value")

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
	c := testarango.RandomString(6, 10)
	_, err = dbh.CreateCollection(c, &driver.CreateCollectionOptions{})
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
	n, err := GenQualifiedAQLFilterStatement(qmap, f)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Equal(n, "FILTER fizz.identifier == 'mahomes@gmail.com'\n OR fizz.identifier == 'brees@gmail.com'", "should match filter statement")
	err = dbh.ValidateQ(genFullQualifiedStmt(n, "fizz", c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item substring for quotes
	qf, err := ParseFilterString("label=~GWDI")
	assert.NoError(err, "should not return any parsing error")
	qs, err := GenQualifiedAQLFilterStatement(qmap, qf)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Equal(qs, "FILTER v.label =~ 'GWDI'", "should contain GWDI substring")
	err = dbh.ValidateQ(genFullQualifiedStmt(qs, "v", c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test date equals
	df, err := ParseFilterString("created_at$==2019,created_at$==2018")
	assert.NoError(err, "should not return any parsing error")
	dn, err := GenQualifiedAQLFilterStatement(qmap, df)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Equal(dn, "FILTER foo.created_at == DATE_ISO8601('2019')\n OR foo.created_at == DATE_ISO8601('2018')")
	err = dbh.ValidateQ(genFullQualifiedStmt(dn, "foo", c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item in array equals
	af, err := ParseFilterString("sport@==basketball")
	assert.NoError(err, "should not return any parsing error")
	an, err := GenQualifiedAQLFilterStatement(qmap, af)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(an, "LET", "should contain LET term, indicating array item")
	assert.Contains(
		an,
		"FILTER 'basketball' IN bar.game[*]",
		"should contain an array containing statement",
	)
	err = dbh.ValidateQ(genFullQualifiedStmt(an, "bar", c))
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
	err = dbh.ValidateQ(genFullQualifiedStmt(an2, "bar", c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item in array not equals
	bf, err := ParseFilterString("sport@!=banana,sport@!=apple")
	assert.NoError(err, "should not return any parsing error")
	bn, err := GenQualifiedAQLFilterStatement(qmap, bf)
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(bn, "NOT IN", "should contain NOT IN statement, indicating item is not in array")
	assert.Contains(bn, "OR", "should contain OR term")
	assert.Contains(
		bn,
		"FILTER 'banana' NOT IN bar.game[*]",
		"should contain filter with NOT IN operator with collection and column name",
	)
	assert.Contains(
		bn,
		"FILTER 'apple' NOT IN bar.game[*]",
		"should contain filter with NOT IN operator with collection and column name",
	)
	err = dbh.ValidateQ(genFullQualifiedStmt(bn, "bar", c))
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
	c := testarango.RandomString(9, 11)
	_, err = dbh.CreateCollection(c, &driver.CreateCollectionOptions{})
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
	// test regular string equals
	s, err := ParseFilterString("email===mahomes@gmail.com,email===brees@gmail.com")
	assert.NoError(err, "should not have any error from parsing string")
	n, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: s, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(n, "FILTER", "should contain FILTER term")
	assert.Contains(n, "doc.email == 'mahomes@gmail.com'", "should contain proper == statement")
	assert.Contains(n, "doc.email == 'brees@gmail.com'", "should contain proper == statement")
	assert.Contains(n, "OR", "should contain OR term")
	err = dbh.ValidateQ(genFullStmt(n, c))
	assert.NoError(err, "should not have any invalid AQL query")
	// AND operator
	s2, err := ParseFilterString("email===mahomes@gmail.com;email===brees@gmail.com")
	assert.NoError(err, "should not have any error from parsing string")
	n2, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: s2, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(n2, "FILTER", "should contain FILTER term")
	assert.Contains(n2, "doc.email == 'mahomes@gmail.com'", "should contain proper == statement")
	assert.Contains(n2, "doc.email == 'brees@gmail.com'", "should contain proper == statement")
	assert.Contains(n2, "AND", "should contain AND term")
	err = dbh.ValidateQ(genFullStmt(n2, c))
	assert.NoError(err, "should not have any invalid AQL query")
	// substring match
	qf, err := ParseFilterString("label=~GWDI")
	assert.NoError(err, "should not return any parsing error")
	qs, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: qf, Doc: "doc"})
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(qs, "FILTER", "should contain FILTER term")
	assert.Contains(qs, "doc.label =~ 'GWDI'", "should contain GWDI substring")
	err = dbh.ValidateQ(genFullStmt(qs, c))
	assert.NoError(err, "should not have any invalid AQL query")
	// substring match with AND operator
	qf2, err := ParseFilterString("label=~GWDI;email===brady@gmail.com")
	assert.NoError(err, "should not return any parsing error")
	qs2, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: qf2, Doc: "doc"})
	assert.NoError(err, "should not return any error when generating AQL filter statement")
	assert.Contains(qs2, "FILTER", "should contain FILTER term")
	assert.Contains(qs2, "doc.label =~ 'GWDI'", "should contain GWDI substring")
	assert.Contains(qs2, "doc.email == 'brady@gmail.com'", "should contain proper == statement")
	assert.Contains(n2, "AND", "should contain AND term")
	err = dbh.ValidateQ(genFullStmt(qs2, c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test date equals
	ds, err := ParseFilterString("created_at$==2019,created_at$>2018")
	assert.NoError(err, "should not have any error from parsing string")
	dn, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: ds, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(dn, "doc.created_at == DATE_ISO8601('2019')", "should contain DATE_ISO8601 term")
	assert.Contains(dn, "doc.created_at > DATE_ISO8601('2018')", "should contain DATE_ISO8601 term")
	assert.Contains(dn, "OR", "should contain OR term")
	err = dbh.ValidateQ(genFullStmt(dn, c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test date equals with AND operator
	ds2, err := ParseFilterString("created_at$<2019;created_at$<=2018;created_at$>=2020")
	assert.NoError(err, "should not have any error from parsing string")
	dn2, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: ds2, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(dn2, "FILTER doc.created_at < DATE_ISO8601('2019')", "should contain DATE_ISO8601 term")
	assert.Contains(dn2, "doc.created_at <= DATE_ISO8601('2018')", "should contain DATE_ISO8601 term")
	assert.Contains(dn2, "doc.created_at >= DATE_ISO8601('2020')", "should contain DATE_ISO8601 term")
	assert.Contains(dn2, "AND", "should contain AND term")
	err = dbh.ValidateQ(genFullStmt(dn, c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item in array equals
	as, err := ParseFilterString("sport@==basketball")
	assert.NoError(err, "should not have any error from parsing string")
	an, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: as, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(an, "LET", "should contain LET term, indicating array item")
	err = dbh.ValidateQ(genFullStmt(an, c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item substring in array
	a, err := ParseFilterString("sport@=~basket")
	assert.NoError(err, "should not have any error from parsing string")
	af, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: a, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(af,
		"FILTER CONTAINS",
		"should contain FILTER CONTAINS statement, indicating array item substring",
	)
	err = dbh.ValidateQ(genFullStmt(af, c))
	assert.NoError(err, "should not have any invalid AQL query")
	// test item in array not equals
	b, err := ParseFilterString("sport@!=banana,sport@!=apple")
	assert.NoError(err, "should not have any error from parsing string")
	bf, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: b, Doc: "doc"})
	assert.NoError(err, "should not have any error from generating AQL filter statement")
	assert.Contains(bf, "NOT IN", "should contain NOT IN statement, indicating item is not in array")
	assert.Contains(bf, "OR", "should contain OR term")
	err = dbh.ValidateQ(genFullStmt(bf, c))
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
