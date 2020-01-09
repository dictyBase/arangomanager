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

var arrToAQL = map[string]string{
	"==": "IN",
	"!=": "NOT IN",
}

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

// StatementParameters is a container for elements needed in the AQL statement
type StatementParameters struct {
	// Map of filters to database fields
	Fmap map[string]string
	// Slice of Filter structs, contains all necessary items for AQL statement
	Filters []*Filter
	// The variable used for looping inside a collection (i.e. the "s" in "FOR s IN stock")
	Doc string
	// The variable used for looping inside a graph (i.e. the "v" in "FOR v IN 1..1 OUTBOUND s GRAPH 'xyz'")
	Vert string
}

//buildFilter regex to capture all variations of filter string
func buildFilter() (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString(`(\w+)`)
	b.WriteString(`(\=\=|\!\=|\=\=\=|\!\=\=|`)
	b.WriteString(`\=\~|\!\~|>|<|>\=|`)
	b.WriteString(`\=<|\$\=\=|\$\>|`)
	b.WriteString(`\$\>\=|\$\<|\$\<\=|`)
	b.WriteString(`\@\=\=|\@\!\=|`)
	b.WriteString(`\@\!\~|\@\=\~)`)
	b.WriteString(`([\w-@.\s]+)(\,|\;)?`)
	return regexp.Compile(b.String())
}

// buildDate builds a regex to capture all variations of date string
// https://play.golang.org/p/NzeBmlQh13v
func buildDate() (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString(`^\d{4}\-(0[1-9]|1[012])$|`)
	b.WriteString(`^\d{4}$|^\d{4}\-(0[1-9]|`)
	b.WriteString(`1[012])\-(0[1-9]|[12][0-9]|3[01])$`)
	return regexp.Compile(b.String())
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
		"=~":  "=~",
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
	var filters []*Filter
	qre, err := buildFilter()
	if err != nil {
		return filters, err
	}
	m := qre.FindAllStringSubmatch(fstr, -1)
	if len(m) == 0 {
		return filters, nil
	}
	omap := getOperatorMap()
	for _, n := range m {
		// if no operator found in map, return slice and throw error
		if _, ok := omap[n[2]]; !ok {
			return filters, fmt.Errorf("filter operator %s not allowed", n[2])
		}
		f := &Filter{
			Field:    n[1],
			Operator: n[2],
			Value:    n[3],
		}
		if len(n) == 5 {
			f.Logic = n[4]
		}
		filters = append(filters, f)
	}
	return filters, nil
}

// GenQualifiedAQLFilterStatement generates an AQL(arangodb query language)
// compatible filter query statement where the fields map is expected to
// contain namespaced(fully qualified like
//		{
//			tag: "doc.label",
//			name: "doc.level.identifier"
//		}
//	)
// mapping to database fields
func GenQualifiedAQLFilterStatement(fmap map[string]string, filters []*Filter) (string, error) {
	lmap := map[string]string{",": "OR", ";": "AND"}
	omap := getOperatorMap()
	dmap := getDateOperatorMap()
	amap := getArrayOperatorMap()
	stmts := make(map[string][]string)
	for _, f := range filters {
		// check if operator is used for array item
		if _, ok := amap[f.Operator]; ok {
			str := randString(10)
			if amap[f.Operator] == "=~" {
				stmts["let"] = append(stmts["let"],
					fmt.Sprintf(`
							LET %s = (
								FOR x IN %s[*]
									FILTER CONTAINS(x, LOWER('%s')) 
									LIMIT 1 
									RETURN 1
							)
						`,
						str,
						fmap[f.Field],
						f.Value,
					),
				)
			} else {
				stmts["let"] = append(stmts["let"],
					fmt.Sprintf(`
							LET %s = (
								FILTER '%s' %s %s[*] 
								RETURN 1
							)
						`,
						str,
						f.Value,
						arrToAQL[amap[f.Operator]],
						fmap[f.Field],
					),
				)
			}
			stmts["nonlet"] = append(stmts["nonlet"], fmt.Sprintf("LENGTH(%s) > 0", str))
		} else if _, ok := dmap[f.Operator]; ok {
			// validate date format
			if err := dateValidator(f.Value); err != nil {
				return "", err
			}
			// write time conversion into AQL query
			stmts["nonlet"] = append(stmts["nonlet"], fmt.Sprintf(
				"%s %s DATE_ISO8601('%s')",
				fmap[f.Field],
				omap[f.Operator],
				f.Value,
			))
		} else {
			// write the rest of AQL statement based on regular string data
			stmts["nonlet"] = append(stmts["nonlet"],
				fmt.Sprintf(
					"%s %s %s",
					fmap[f.Field],
					omap[f.Operator],
					checkAndQuote(f.Operator, f.Value),
				))
			// if there's logic, write that too
		}
		// if there's logic, write that too
		if len(f.Logic) != 0 {
			stmts["nonlet"] = append(stmts["nonlet"], fmt.Sprintf("\n %s ", lmap[f.Logic]))
		}
	}
	return toFullStatement(stmts), nil
}

// GenAQLFilterStatement generates an AQL(arangodb query language) compatible
// filter query statement
func GenAQLFilterStatement(p *StatementParameters) (string, error) {
	fmap := p.Fmap
	filters := p.Filters
	doc := p.Doc
	vert := p.Vert
	lmap := map[string]string{",": "OR", ";": "AND"}
	omap := getOperatorMap()
	dmap := getDateOperatorMap()
	amap := getArrayOperatorMap()
	stmts := arraylist.New()
	var inner string
	if len(vert) > 0 {
		inner = vert
	} else {
		inner = doc
	}
	for _, f := range filters {
		// check if operator is used for array item
		if _, ok := amap[f.Operator]; ok {
			str := randString(10)
			if amap[f.Operator] == "=~" {
				stmts.Insert(0,
					fmt.Sprintf(`
							LET %s = (
								FOR x IN %s.%s[*]
									FILTER CONTAINS(x, LOWER('%s')) 
									LIMIT 1 
									RETURN 1
							)
						`,
						str,
						inner,
						fmap[f.Field],
						f.Value,
					),
				)
			}
			if amap[f.Operator] == "==" {
				stmts.Insert(0,
					fmt.Sprintf(`
							LET %s = (
								FILTER '%s' IN %s.%s[*] 
								RETURN 1
							)
						`,
						str,
						f.Value,
						inner,
						fmap[f.Field],
					),
				)
			}
			if amap[f.Operator] == "!=" {
				stmts.Insert(0,
					fmt.Sprintf(`
							LET %s = (
								FILTER '%s' NOT IN %s.%s[*]
								RETURN 1
							)
						`,
						str,
						f.Value,
						inner,
						fmap[f.Field],
					),
				)
			}
			stmts.Add(fmt.Sprintf("LENGTH(%s) > 0", str))
			// if there's logic, write that too
			if len(f.Logic) != 0 {
				stmts.Add(fmt.Sprintf("\n %s ", lmap[f.Logic]))
			}
		} else if _, ok := dmap[f.Operator]; ok {
			if err := dateValidator(f.Value); err != nil {
				return "", err
			}
			// write time conversion into AQL query
			stmts.Add(fmt.Sprintf(
				"%s.%s %s DATE_ISO8601('%s')",
				inner,
				fmap[f.Field],
				omap[f.Operator],
				f.Value,
			))
			if len(f.Logic) != 0 {
				stmts.Add(fmt.Sprintf("\n %s ", lmap[f.Logic]))
			}
		} else {
			// write the rest of AQL statement based on regular string data
			stmts.Add(
				fmt.Sprintf(
					"%s.%s %s %s",
					inner,
					fmap[f.Field],
					omap[f.Operator],
					checkAndQuote(f.Operator, f.Value),
				),
			)
			if len(f.Logic) != 0 {
				stmts.Add(fmt.Sprintf("\n %s ", lmap[f.Logic]))
			}
		}
	}
	return toString(stmts), nil
}

func toFullStatement(m map[string][]string) string {
	var clause strings.Builder
	if v, ok := m["let"]; ok {
		clause.WriteString(strings.Join(v, ""))
	}
	clause.WriteString(" FILTER ")
	if v, ok := m["nonlet"]; ok {
		clause.WriteString(strings.Join(v, ""))
	}
	return clause.String()
}

func toString(l *arraylist.List) string {
	var clause strings.Builder
	it := l.Iterator()
	for it.Next() {
		// print all LET statements first
		if strings.Contains(it.Value().(string), "LET ") {
			clause.WriteString(it.Value().(string))
		}
	}
	// start FILTER statement
	clause.WriteString("FILTER ")
	it.Begin()
	for it.Next() {
		// print all non-LET statements
		if !strings.Contains(it.Value().(string), "LET ") {
			clause.WriteString(it.Value().(string))
		}
	}
	return clause.String()
}

// check if operator is used for a string
func checkAndQuote(op, value string) string {
	if op == "==" || op == "===" || op == "!=" || op == "~" || op == "!~" {
		return fmt.Sprintf("'%s'", value)
	}
	return value
}

func dateValidator(s string) error {
	// get all regex matches for date
	dre, err := buildDate()
	if err != nil {
		return err
	}
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
