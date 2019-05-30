package query

import (
	"fmt"
	"log"
	"testing"

	driver "github.com/arangodb/go-driver"
	"github.com/dictyBase/arangomanager/testarango"

	"github.com/stretchr/testify/assert"
)

// mapping of filters to database fields
var fmap = map[string]string{
	"created_at": "created_at",
	"sport":      "sports",
	"email":      "email",
}

var qmap = map[string]string{
	"created_at": "foo.created_at",
	"sport":      "bar.game",
	"email":      "fizz.identifier",
}

var gta *testarango.TestArango

func TestParseFilterString(t *testing.T) {
	s, err := ParseFilterString("sport===football;email===mahomes@gmail.com")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}

	assert := assert.New(t)
	assert.Equal(len(s), 2, "should match length of two items in filter array")

	b, err := ParseFilterString("xyz")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	assert.Equal(len(b), 0, "should have empty slice since regex doesn't match string")
}

func TestGenQualifiedAQLFilterStatement(t *testing.T) {
	// test regular string equals
	f, err := ParseFilterString("email===mahomes@gmail.com,email===brees@gmail.com")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	n, err := GenQualifiedAQLFilterStatement(qmap, f)
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert := assert.New(t)
	assert.Contains(n, "FILTER", "should contain FILTER term")
	assert.Contains(n, "fizz.identifier == 'mahomes@gmail.com'", "should contain proper == statement")
	assert.Contains(n, "fizz.identifier == 'brees@gmail.com'", "should contain proper == statement")
	assert.Contains(n, "OR", "should contain OR term")

	// test date equals
	df, err := ParseFilterString("created_at$==2019,created_at$==2018")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	dn, err := GenQualifiedAQLFilterStatement(qmap, df)
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(dn, "DATE_ISO8601", "should contain DATE_ISO8601 term")
	assert.Contains(dn, "OR", "should contain OR term")
	assert.Contains(
		dn,
		"foo.created_at == DATE_ISO8601('2018')",
		"should contain proper date statement",
	)
	// test item in array equals
	af, err := ParseFilterString("sport@==basketball")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	an, err := GenQualifiedAQLFilterStatement(qmap, af)
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(an, "LET", "should contain LET term, indicating array item")
	assert.Contains(
		an,
		"FILTER 'basketball' IN bar.game[*]",
		"should contain an array containing statement",
	)
	// test item substring in array
	af2, err := ParseFilterString("sport@=~basket")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	an2, err := GenQualifiedAQLFilterStatement(qmap, af2)
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(
		an2,
		"FILTER CONTAINS(x, LOWER('basket'))",
		"should contain FILTER CONTAINS statement, indicating array item substring",
	)
	// test item in array not equals
	bf, err := ParseFilterString("sport@!=banana,sport@!=apple")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	bn, err := GenQualifiedAQLFilterStatement(qmap, bf)
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
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
}

func TestGenAQLFilterStatement(t *testing.T) {
	ta, err := testarango.NewTestArangoFromEnv(true)
	if err != nil {
		log.Fatalf("unable to construct new TestArango instance %s", err)
	}
	gta = ta
	dbh, err := ta.DB(ta.Database)
	if err != nil {
		log.Fatalf("unable to get database %s", err)
	}
	c := "test_collection"
	_, err = dbh.CreateCollection(c, &driver.CreateCollectionOptions{})
	if err != nil {
		e := dbh.Drop()
		if e != nil {
			log.Fatalf("could not remove database %s", e)
		}
		log.Fatalf("unable to create collection %s %s", c, err)
	}
	defer func() {
		e := dbh.Drop()
		if e != nil {
			log.Fatalf("could not remove database %s", e)
		}
	}()
	// test regular string equals
	s, err := ParseFilterString("email===mahomes@gmail.com,email===brees@gmail.com")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	n, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: s, Doc: "doc"})
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert := assert.New(t)
	assert.Contains(n, "FILTER", "should contain FILTER term")
	assert.Contains(n, "doc.email == 'mahomes@gmail.com'", "should contain proper == statement")
	assert.Contains(n, "OR", "should contain OR term")
	x := dbh.ValidateQ(genFullStmt(n))
	if x != nil {
		t.Fatalf("invalid AQL query %s", x)
	}
	t.Log(n)

	// test date equals
	ds, err := ParseFilterString("created_at$==2019,created_at$==2018")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	dn, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: ds, Doc: "doc"})
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(dn, "DATE_ISO8601", "should contain DATE_ISO8601 term")
	assert.Contains(dn, "OR", "should contain OR term")
	xd := dbh.ValidateQ(genFullStmt(dn))
	if xd != nil {
		t.Fatalf("invalid AQL query %s", dn)
	}
	t.Log(dn)

	// test item in array equals
	as, err := ParseFilterString("sport@==basketball")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	an, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: as, Doc: "doc"})
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(an, "LET", "should contain LET term, indicating array item")
	xa := dbh.ValidateQ(genFullStmt(an))
	if xa != nil {
		t.Fatalf("invalid AQL query %s", xa)
	}
	t.Log(an)

	// test item substring in array
	a, err := ParseFilterString("sport@=~basket")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	af, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: a, Doc: "doc"})
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(af, "FILTER CONTAINS", "should contain FILTER CONTAINS statement, indicating array item substring")
	xaf := dbh.ValidateQ(genFullStmt(af))
	if xaf != nil {
		t.Fatalf("invalid AQL query %s", xaf)
	}
	t.Log(af)

	// test item in array not equals
	b, err := ParseFilterString("sport@!=banana,sport@!=apple")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	bf, err := GenAQLFilterStatement(&StatementParameters{Fmap: fmap, Filters: b, Doc: "doc"})
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(bf, "NOT IN", "should contain NOT IN statement, indicating item is not in array")
	assert.Contains(bf, "OR", "should contain OR term")
	xb := dbh.ValidateQ(genFullStmt(bf))
	if xb != nil {
		t.Fatalf("invalid AQL query %s", xb)
	}
	t.Log(bf)
}

func genFullStmt(f string) string {
	return fmt.Sprintf(
		`
		FOR doc in test_collection
			%s
			RETURN doc
		`,
		f,
	)
}
