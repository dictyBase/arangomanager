package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// mapping of filters to database fields
var fmap = map[string]string{
	"created_at": "created_at",
	"sport":      "sports",
	"email":      "email",
}

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

func TestGenAQLFilterStatement(t *testing.T) {
	// test regular string equals
	s, err := ParseFilterString("email===mahomes@gmail.com,email===brees@gmail.com")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	n, err := GenAQLFilterStatement(fmap, s, "doc")
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert := assert.New(t)
	assert.Contains(n, "FILTER", "should contain FILTER term")
	assert.Contains(n, "doc.email == 'mahomes@gmail.com'", "should contain proper == statement")

	// test date equals
	ds, err := ParseFilterString("created_at$==2019,created_at$==2018")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	dn, err := GenAQLFilterStatement(fmap, ds, "doc")
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(dn, "DATE_ISO8601", "should contain DATE_ISO8601 term")

	// test item in array equals
	as, err := ParseFilterString("sport@==basketball")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	an, err := GenAQLFilterStatement(fmap, as, "doc")
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(an, "LET", "should contain LET term, indicating array item")

	// test item substring in array
	a, err := ParseFilterString("sport@=~basket")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	af, err := GenAQLFilterStatement(fmap, a, "doc")
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(af, "FILTER CONTAINS", "should contain FILTER CONTAINS statement, indicating array item substring")

	// test item in array not equals
	b, err := ParseFilterString("sport@!=banana,sport@!=apple")
	if err != nil {
		t.Fatalf("error in parsing filter string %s", err)
	}
	bf, err := GenAQLFilterStatement(fmap, b, "doc")
	if err != nil {
		t.Fatalf("error in generating AQL filter statement %s", err)
	}
	assert.Contains(bf, "NOT IN", "should contain NOT IN statement, indicating item is not in array")
}
