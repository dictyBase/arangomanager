package query

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/jinzhu/now"
)

/**
* TO DO:
* 1) Return FILTER statement all in one line without redundant FILTER commands
 */

// regex to capture all variations of filter string
var qre = regexp.MustCompile(`(\w+)(\=\=|\!\=|\=\=\=|\!\=\=|\~|\!\~|>|<|>\=|\=<|\$\=\=|\$\>|\$\>\=|\$\<|\$\<\=|\@\=\=|\@\!\=|\@\!\~|\@\=\~)([\w-]+)(\,|\;)?`)

// regex to capture all variations of date string
// https://play.golang.org/p/NzeBmlQh13v
var dre = regexp.MustCompile(`^\d{4}\-(0[1-9]|1[012])$|^\d{4}$|^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$`)

// Filter is a container for filter parameters
type Filter struct {
	// Field of the object on which the filter will be applied
	Field string
	// Type of filter for matching or exclusion
	Operator string
	// The value to match or exclude
	Value string
	// Logic for combining multiple filter expressions, usually "AND" or "OR"
	Logic string
}

func getOperatorMap() map[string]string {
	return map[string]string{
		"==":  "==",
		"===": "==",
		"!=":  "!=",
		">":   ">",
		"<":   "<",
		">=":  ">=",
		"<=":  "<=",
		"~":   "=~",
		"!~":  "!~",
		"$==": "==",
		"$>":  ">",
		"$<":  "<",
		"$>=": ">=",
		"$<=": "<=",
		"@==": "==",
		"@=~": "=~",
		"@!~": "!~",
		"@!=": "!=",
	}
}

// map values that are predefined as dates
func getDateOperatorMap() map[string]string {
	return map[string]string{
		"$==": "==",
		"$>":  ">",
		"$<":  "<",
		"$>=": ">=",
		"$<=": "<=",
	}
}

// map values that are predefined as array items
func getArrayOperatorMap() map[string]string {
	return map[string]string{
		"@==": "==",
		"@=~": "=~",
		"@!~": "!~",
		"@!=": "!=",
	}
}

// ParseFilterString parses a predefined filter string to Filter
// structure. The filter string specification is defined in
// corresponding protocol buffer definition.
func ParseFilterString(fstr string) ([]*Filter, error) {
	// create slice that will contain Filter structs
	var filters []*Filter
	// get all regex matches for fstr
	m := qre.FindAllStringSubmatch(fstr, -1)
	// if no matches, return empty slice
	if len(m) == 0 {
		return filters, nil
	}
	// get map of all allowed operators
	omap := getOperatorMap()
	// loop through separate items from fstr string
	for _, n := range m {
		// if no operator found in map, return slice and throw error
		if _, ok := omap[n[2]]; !ok {
			return filters, fmt.Errorf("filter operator %s not allowed", n[2])
		}
		// initialize Filter container with appropriate data
		f := &Filter{
			Field:    n[1],
			Operator: n[2],
			Value:    n[3],
		}
		if len(n) == 5 {
			f.Logic = n[4]
		}
		// add this Filter to slice
		filters = append(filters, f)
	}
	// return slice of Filter structs
	return filters, nil
}

// GenAQLFilterStatement generates an AQL(arangodb query language) compatible
// filter query statement
func GenAQLFilterStatement(fmap map[string]string, filters []*Filter, doc string) (string, error) {
	// set map for logic
	lmap := map[string]string{",": "OR", ";": "AND"}
	// get map of all allowed operators
	omap := getOperatorMap()
	// get map of all date operators
	dmap := getDateOperatorMap()
	// get map of all array operators
	amap := getArrayOperatorMap()
	// initialize variable for stmts slice
	stmts := arraylist.New()
	// loop over items in filters slice
	for _, f := range filters {
		// check if operator is used for array item
		if _, ok := amap[f.Operator]; ok {
			str := randString(10)
			// write the rest of AQL statement based on array data
			stmts.Insert(0,
				fmt.Sprintf(`
					LET %s = (
						FOR x IN %s.%s[*]
							FILTER CONTAINS(x, LOWER('%s')) 
							LIMIT 1 
							RETURN 1
					)
					FILTER LENGTH(%s) > 0 
				`,
					str,
					doc,
					fmap[f.Field],
					f.Value,
					str,
				),
			)
		} else if _, ok := dmap[f.Operator]; ok {
			// validate date format
			if err := dateValidator(f.Value); err != nil {
				return "", err
			}
			// write time conversion into AQL query
			stmts.Add(fmt.Sprintf(
				"FILTER %s.%s %s DATE_ISO8601('%s')",
				doc,
				fmap[f.Field],
				omap[f.Operator],
				f.Value,
			))
			// if there's logic, write that too
			if len(f.Logic) != 0 {
				stmts.Add(fmt.Sprintf(" %s ", lmap[f.Logic]))
			}
		} else {
			// write the rest of AQL statement based on non-date data
			stmts.Add(
				fmt.Sprintf(
					"FILTER %s.%s %s %s",
					doc,
					fmap[f.Field],
					omap[f.Operator],
					checkAndQuote(f.Operator, f.Value),
				),
			)
			// if there's logic, write that too
			if len(f.Logic) != 0 {
				stmts.Add(fmt.Sprintf(" %s ", lmap[f.Logic]))
			}
		}
	}
	return toString(stmts), nil
}

func toString(l *arraylist.List) string {
	var clause strings.Builder
	it := l.Iterator()
	for it.Next() {
		// it returns interface{}
		clause.WriteString(it.Value().(string))
	}
	return clause.String()
}

// check if operator is used for a string
func checkAndQuote(op, value string) string {
	if op == "===" || op == "!==" || op == "=~" || op == "!~" {
		return fmt.Sprintf("'%s'", value)
	}
	return value
}

func dateValidator(s string) error {
	// get all regex matches for date
	m := dre.FindString(s)
	if len(m) == 0 {
		return fmt.Errorf("error in validating date %s", s)
	}
	// grab valid date and parse to time object
	if _, err := now.Parse(m); err != nil {
		return fmt.Errorf("could not parse date %s %s", s, err)
	}
	return nil
}

const (
	charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

var seedRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func stringWithCharset(length int, charset string) string {
	var b []byte
	for i := 0; i < length; i++ {
		b = append(
			b,
			charset[seedRand.Intn(len(charset))],
		)
	}
	return string(b)
}

func randString(length int) string {
	return stringWithCharset(length, charSet)
}
