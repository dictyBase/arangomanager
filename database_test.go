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
	c := setup(adbh, t)
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
