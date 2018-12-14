package query

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/jinzhu/now"
)

// regex to capture all variations of filter string
var qre = regexp.MustCompile(`(\w+)(\=\=|\!\=|\=\=\=|\!\=\=|\~|\!\~|>|<|>\=|\=<|\$\=\=|\$\>|\$\>\=|\$\<|\$\<\=)([\w-]+)(\,|\;)?`)

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
func GenAQLFilterStatement(fmap map[string]string, filters []*Filter) (string, error) {
	// set map for logic
	lmap := map[string]string{",": "OR", ";": "AND"}
	// get map of all allowed operators
	omap := getOperatorMap()
	// get map of all date operators
	dmap := getDateOperatorMap()
	// initialize variable for a string builder
	var clause strings.Builder
	// write FILTER to this string
	clause.WriteString("FILTER ")
	// loop over items in filters slice
	for _, f := range filters {
		// check if operator is for a date
		if _, ok := dmap[f.Operator]; ok {
			// validate date format
			if err := dateValidator(f.Value); err != nil {
				return "", err
			}
			// write time conversion into AQL query
			clause.WriteString(
				fmt.Sprintf(
					"%s %s DATE_ISO8601('%s')",
					fmap[f.Field],
					omap[f.Operator],
					f.Value,
				),
			)
			// if there's logic, write that too
			if len(f.Logic) != 0 {
				clause.WriteString(fmt.Sprintf(" %s ", lmap[f.Logic]))
			}
		} else {
			// write the rest of AQL statement based on non-date data
			clause.WriteString(
				fmt.Sprintf(
					"%s %s %s",
					fmap[f.Field],
					omap[f.Operator],
					checkAndQuote(f.Operator, f.Value),
				),
			)
			// if there's logic, write that too
			if len(f.Logic) != 0 {
				clause.WriteString(fmt.Sprintf(" %s ", lmap[f.Logic]))
			}
		}
	}
	// return the string
	return clause.String(), nil
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
