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

const (
	filterStrLen = 5
	strSeedLen   = 10
	arrMatchTmpl = `
	      LET %s = (
	 		FOR x IN %s.%s[*]
				FILTER CONTAINS(x, LOWER('%s')) 
				LIMIT 1 
				RETURN 1
		)
	`
	arrEqualTmpl = `
		LET %s = (
				FILTER '%s' IN %s.%s[*] 
				RETURN 1
		)
	`
	arrNotEqualTmpl = `
		LET %s = (
				FILTER '%s' NOT IN %s.%s[*]
				RETURN 1
		)
	`
	dateTmpl = "%s.%s %s DATE_ISO8601('%s')"
)

var arrToAQL = map[string]string{
	"==": "IN",
	"!=": "NOT IN",
}

// Filter is a container for filter parameters.
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

// StatementParameters is a container for elements needed in the AQL statement.
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

// buildFilter regex to capture all variations of filter string.
func buildFilter() (*regexp.Regexp, error) {
	var bldr strings.Builder
	bldr.WriteString(`(\w+)`)
	bldr.WriteString(`(\=\=|\!\=|\=\=\=|\!\=\=|`)
	bldr.WriteString(`\=\~|\!\~|>|<|>\=|`)
	bldr.WriteString(`\=<|\$\=\=|\$\>|`)
	bldr.WriteString(`\$\>\=|\$\<|\$\<\=|`)
	bldr.WriteString(`\@\=\=|\@\!\=|`)
	bldr.WriteString(`\@\!\~|\@\=\~)`)
	bldr.WriteString(`([\w-@.\s]+)(\,|\;)?`)
	rgxp, err := regexp.Compile(bldr.String())
	if err != nil {
		return rgxp, fmt.Errorf("error in compiling regexp %s", err)
	}

	return rgxp, nil
}

// buildDate builds a regex to capture all variations of date string
// https://play.golang.org/p/NzeBmlQh13v
func buildDate() (*regexp.Regexp, error) {
	var bldr strings.Builder
	bldr.WriteString(`^\d{4}\-(0[1-9]|1[012])$|`)
	bldr.WriteString(`^\d{4}$|^\d{4}\-(0[1-9]|`)
	bldr.WriteString(`1[012])\-(0[1-9]|[12][0-9]|3[01])$`)
	rgxp, err := regexp.Compile(bldr.String())
	if err != nil {
		return rgxp, fmt.Errorf("error in compiling regexp %s", err)
	}

	return rgxp, nil
}

func getLogic(input string) string {
	lmap := map[string]string{",": "OR", ";": "AND"}

	return lmap[input]
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

func getOperator(opt string) string {
	omap := getOperatorMap()

	return omap[opt]
}

func getArrayOpertaor(opt string) string {
	amap := getArrayOperatorMap()

	return amap[opt]
}

func getDateOperator(opt string) string {
	dmap := getDateOperatorMap()

	return dmap[opt]
}

func hasOperator(opt string) bool {
	omap := getOperatorMap()
	_, isok := omap[opt]

	return isok
}

func hasDateOperator(opt string) bool {
	dmap := getDateOperatorMap()
	_, isok := dmap[opt]

	return isok
}

func hasArrayOperator(opt string) bool {
	amap := getArrayOperatorMap()
	_, isok := amap[opt]

	return isok
}

// map values that are predefined as dates.
func getDateOperatorMap() map[string]string {
	return map[string]string{
		"$==": "==",
		"$>":  ">",
		"$<":  "<",
		"$>=": ">=",
		"$<=": "<=",
	}
}

// map values that are predefined as array items.
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
	filters := make([]*Filter, 0)
	qre, err := buildFilter()
	if err != nil {
		return filters, err
	}
	m := qre.FindAllStringSubmatch(fstr, -1)
	if len(m) == 0 {
		return filters, nil
	}
	omap := getOperatorMap()
	for _, mtc := range m {
		// if no operator found in map, return slice and throw error
		if _, ok := omap[mtc[2]]; !ok {
			return filters, fmt.Errorf("filter operator %s not allowed", mtc[2])
		}
		flt := &Filter{
			Field:    mtc[1],
			Operator: mtc[2],
			Value:    mtc[3],
		}
		if len(mtc) == filterStrLen {
			flt.Logic = mtc[4]
		}
		filters = append(filters, flt)
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
// mapping to database fields.
func GenQualifiedAQLFilterStatement(fmap map[string]string, filters []*Filter) (string, error) {
	lmap := map[string]string{",": "OR", ";": "AND"}
	omap := getOperatorMap()
	dmap := getDateOperatorMap()
	amap := getArrayOperatorMap()
	stmts := make(map[string][]string)
	for _, flt := range filters {
		// check if operator is used for array item
		if _, ok := amap[flt.Operator]; ok {
			str := randString(strSeedLen)
			if amap[flt.Operator] == "=~" {
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
						fmap[flt.Field],
						flt.Value,
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
						flt.Value,
						arrToAQL[amap[flt.Operator]],
						fmap[flt.Field],
					),
				)
			}
			stmts["nonlet"] = append(stmts["nonlet"], fmt.Sprintf("LENGTH(%s) > 0", str))
		} else if _, ok := dmap[flt.Operator]; ok {
			// validate date format
			if err := dateValidator(flt.Value); err != nil {
				return "", err
			}
			// write time conversion into AQL query
			stmts["nonlet"] = append(stmts["nonlet"], fmt.Sprintf(
				"%s %s DATE_ISO8601('%s')",
				fmap[flt.Field],
				omap[flt.Operator],
				flt.Value,
			))
		} else {
			// write the rest of AQL statement based on regular string data
			stmts["nonlet"] = append(stmts["nonlet"],
				fmt.Sprintf(
					"%s %s %s",
					fmap[flt.Field],
					omap[flt.Operator],
					addQuoteToStrings(flt.Operator, flt.Value),
				))
			// if there's logic, write that too
		}
		// if there's logic, write that too
		if len(flt.Logic) != 0 {
			stmts["nonlet"] = append(stmts["nonlet"], fmt.Sprintf("\n %s ", lmap[flt.Logic]))
		}
	}

	return toFullStatement(stmts), nil
}

// GenAQLFilterStatement generates an AQL(arangodb query language) compatible
// filter query statement.
func GenAQLFilterStatement(prms *StatementParameters) (string, error) {
	fmap := prms.Fmap
	inner := prms.Doc
	stmts := arraylist.New()
	if len(prms.Vert) > 0 {
		inner = prms.Vert
	}
	for _, flt := range prms.Filters {
		switch {
		case hasArrayOperator(flt.Operator):
			str := randString(strSeedLen)
			switch getArrayOpertaor(flt.Operator) {
			case "=~":
				stmts.Insert(0, fmt.Sprintf(arrMatchTmpl, str, inner, fmap[flt.Field], flt.Value))
			case "==":
				stmts.Insert(0, fmt.Sprintf(arrEqualTmpl, str, flt.Value, inner, fmap[flt.Field]))
			case "!=":
				stmts.Insert(
					0,
					fmt.Sprintf(arrNotEqualTmpl, str, flt.Value, inner, fmap[flt.Field]),
				)
			}
			stmts.Add(fmt.Sprintf("LENGTH(%s) > 0", str))
		case hasDateOperator(flt.Operator):
			if err := dateValidator(flt.Value); err != nil {
				return "", err
			}
			// write time conversion into AQL query
			stmts.Add(
				fmt.Sprintf(dateTmpl, inner, fmap[flt.Field], getOperator(flt.Operator), flt.Value),
			)
		case hasOperator(flt.Operator):
			// write the rest of AQL statement based on regular string data
			stmts.Add(fmt.Sprintf(
				"%s.%s %s %s", inner, fmap[flt.Field],
				getOperator(flt.Operator), addQuoteToStrings(flt.Operator, flt.Value),
			))
		default:
			return "", fmt.Errorf("unknown opertaor for parsing %s", flt.Operator)
		}
		if len(flt.Logic) > 0 {
			stmts.Add(fmt.Sprintf("\n %s ", getLogic(flt.Logic)))
		}
	}

	return toString(stmts), nil
}

func toFullStatement(mst map[string][]string) string {
	var clause strings.Builder
	if v, ok := mst["let"]; ok {
		clause.WriteString(strings.Join(v, ""))
	}
	clause.WriteString("FILTER ")
	if v, ok := mst["nonlet"]; ok {
		clause.WriteString(strings.Join(v, ""))
	}

	return clause.String()
}

func toString(l *arraylist.List) string {
	var clause strings.Builder
	itr := l.Iterator()
	for itr.Next() {
		// print all LET statements first
		if strings.Contains(itr.Value().(string), "LET ") {
			clause.WriteString(itr.Value().(string))
		}
	}
	// start FILTER statement
	clause.WriteString("FILTER ")
	itr.Begin()
	for itr.Next() {
		// print all non-LET statements
		if !strings.Contains(itr.Value().(string), "LET ") {
			clause.WriteString(itr.Value().(string))
		}
	}

	return clause.String()
}

// check if operator is used for a string.
func addQuoteToStrings(ops, value string) string {
	var stringOperators = map[string]int{
		"==":  1,
		"===": 1,
		"!=":  1,
		"=~":  1,
		"!~":  1,
	}
	if _, ok := stringOperators[ops]; ok {
		return fmt.Sprintf("'%s'", value)
	}

	return value
}

func dateValidator(str string) error {
	// get all regex matches for date
	dre, err := buildDate()
	if err != nil {
		return err
	}
	m := dre.FindString(str)
	if len(m) == 0 {
		return fmt.Errorf("error in validating date %s", str)
	}
	// grab valid date and parse to time object
	if _, err := now.Parse(m); err != nil {
		return fmt.Errorf("could not parse date %s %s", str, err)
	}

	return nil
}

const (
	charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

var seedRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func stringWithCharset(length int, charset string) string {
	var byt []byte
	for i := 0; i < length; i++ {
		byt = append(
			byt,
			charset[seedRand.Intn(len(charset))],
		)
	}

	return string(byt)
}

func randString(length int) string {
	return stringWithCharset(length, charSet)
}
