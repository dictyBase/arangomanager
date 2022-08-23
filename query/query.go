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
	logicIdx         = 2
	charSet          = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	filterStrLen     = 5
	strSeedLen       = 10
	arrQualMatchTmpl = `
		LET %s = (
			FOR x IN %s[*]
				FILTER CONTAINS(x, LOWER('%s')) 
				LIMIT 1 
				RETURN 1
		)
	`
	arrMatchTmpl = `
	      LET %s = (
	 		FOR x IN %s.%s[*]
				FILTER CONTAINS(x, LOWER('%s')) 
				LIMIT 1 
				RETURN 1
		)
	`
	arrQualEqualTmpl = `
		LET %s = (
			FILTER '%s' IN %s[*] 
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
	arrQualNotEqualTmpl = `
		LET %s = (
				FILTER '%s' NOT IN %s[*]
				RETURN 1
		)
	`
	dateTmpl = "%s.%s %s DATE_ISO8601('%s')"
)

var seedRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
var startPrefixRegxp = regexp.MustCompile(`\(`)
var endPrefixRegxp = regexp.MustCompile(`\)`)

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
//
//		{
//			tag: "doc.label",
//			name: "doc.level.identifier"
//		}
//	)
//
// mapping to database fields.
func GenQualifiedAQLFilterStatement(
	fmap map[string]string,
	filters []*Filter,
) (string, error) {
	stmts := map[string]*arraylist.List{
		"let":    arraylist.New(),
		"nonlet": arraylist.New(),
	}
	for _, flt := range filters {
		switch {
		case hasArrayOperator(flt.Operator):
			randStr := randString(strSeedLen)
			switch getArrayOpertaor(flt.Operator) {
				stmts["let"].Insert(
					0,
					fmt.Sprintf(
						arrQualMatchTmpl,
						randStr,
						fmap[flt.Field],
						flt.Value,
					),
				)
			case "==":
				stmts["let"].Insert(
					0,
					fmt.Sprintf(
						arrQualEqualTmpl,
						randStr,
						flt.Value,
						fmap[flt.Field],
					),
				)
			case "!=":
				stmts["let"].Insert(
					0,
					fmt.Sprintf(
						arrQualNotEqualTmpl,
						randStr,
						flt.Value,
						fmap[flt.Field],
					))
			}
			stmts["nonlet"].Add(fmt.Sprintf("LENGTH(%s) > 0", randStr))
		case hasDateOperator(flt.Operator):
			if err := dateValidator(flt.Value); err != nil {
				return "", err
			}
			// write time conversion into AQL query
			stmts["nonlet"].Add(fmt.Sprintf("%s %s DATE_ISO8601('%s')",
				fmap[flt.Field], getOperator(flt.Operator), flt.Value,
			))
		case hasOperator(flt.Operator):
			// write the rest of AQL statement based on regular string data
			stmts["nonlet"].Add(fmt.Sprintf(
				"%s %s %s",
				fmap[flt.Field], getOperator(flt.Operator),
				addQuoteToStrings(flt.Operator, flt.Value),
			))
			// if there's logic, write that too
		default:
			return "", fmt.Errorf(
				"unknown opertaor for parsing %s",
				flt.Operator,
			)
		}
		// if there's logic, write that too
		addLogic(stmts["nonlet"], flt)
	}

	return toFullStatement(stmts), nil
}

// GenAQLFilterStatement generates an AQL(arangodb query language) compatible
// filter query statement.
func GenAQLFilterStatement(prms *StatementParameters) (string, error) {
	inner := prms.Doc
	stmts := arraylist.New()
	if len(prms.Vert) > 0 {
		inner = prms.Vert
	}
	for _, flt := range prms.Filters {
		switch {
		case hasArrayOperator(flt.Operator):
			randStr := randString(strSeedLen)
			switch getArrayOpertaor(flt.Operator) {
			case "=~":
				stmts.Insert(
					0,
					fmt.Sprintf(
						arrMatchTmpl,
						randStr,
						inner,
						prms.Fmap[flt.Field],
						flt.Value,
					),
				)
			case "==":
				stmts.Insert(
					0,
					fmt.Sprintf(
						arrEqualTmpl,
						randStr,
						flt.Value,
						inner,
						prms.Fmap[flt.Field],
					),
				)
			case "!=":
				stmts.Insert(
					0,
					fmt.Sprintf(
						arrNotEqualTmpl,
						randStr,
						flt.Value,
						inner,
						prms.Fmap[flt.Field],
					),
				)
			}
			stmts.Add(fmt.Sprintf("LENGTH(%s) > 0", randStr))
		case hasDateOperator(flt.Operator):
			if err := dateValidator(flt.Value); err != nil {
				return "", err
			}
			// write time conversion into AQL query
			stmts.Add(
				fmt.Sprintf(
					dateTmpl, inner, prms.Fmap[flt.Field],
					getOperator(flt.Operator), flt.Value,
				),
			)
		case hasOperator(flt.Operator):
			// write the rest of AQL statement based on regular string data
			stmts.Add(fmt.Sprintf(
				"%s.%s %s %s",
				inner,
				prms.Fmap[flt.Field],
				getOperator(
					flt.Operator,
				),
				addQuoteToStrings(flt.Operator, flt.Value),
			))
		default:
			return "", fmt.Errorf(
				"unknown opertaor for parsing %s",
				flt.Operator,
			)
		}
		addLogic(stmts, flt)
	}

	return toString(stmts), nil
}

func addLogic(stmts *arraylist.List, flt *Filter) {
	currSize := stmts.Size()
	if len(flt.Logic) == 0 {
		addClosingParen(stmts, currSize)

		return
	}
	logic := getLogic(flt.Logic)
	switch logic {
	case "OR":
		addStartingParen(stmts, currSize)
	case "AND":
		addClosingParen(stmts, currSize)
	}
	stmts.Add(fmt.Sprintf("\n %s ", logic))
}

func addStartingParen(stmts *arraylist.List, currSize int) {
	if !isBalancedParens(stmts) {
		return
	}
	stmts.Insert(currSize-1, " ( ")
}

func addClosingParen(stmts *arraylist.List, currSize int) {
	if isBalancedParens(stmts) {
		return
	}
	elem, _ := stmts.Get(currSize - logicIdx)
	if val, ok := elem.(string); ok {
		if strings.TrimSpace(val) == "OR" {
			stmts.Add(" ) ")
		}
	}
}

func isBalancedParens(stmts *arraylist.List) bool {
	strStmt := stmts.String()
	startLen := len(startPrefixRegxp.FindAllString(strStmt, -1))
	endLen := len(endPrefixRegxp.FindAllString(strStmt, -1))

	return startLen == endLen
}

func toFullStatement(mst map[string]*arraylist.List) string {
	var clause strings.Builder
	if v, ok := mst["let"]; ok {
		clause.WriteString(v.String())
	}
	clause.WriteString("FILTER ")
	if v, ok := mst["nonlet"]; ok {
		clause.WriteString(v.String())
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
	mtch := dre.FindString(str)
	if len(mtch) == 0 {
		return fmt.Errorf("error in validating date %s", str)
	}
	// grab valid date and parse to time object
	if _, err := now.Parse(mtch); err != nil {
		return fmt.Errorf("could not parse date %s %s", str, err)
	}

	return nil
}

func stringWithCharset(length int, charset string) string {
	var byt []byte
	for i := 0; i < length; i++ {
		byt = append(byt, charset[seedRand.Intn(len(charset))])
	}

	return string(byt)
}

func randString(length int) string {
	return stringWithCharset(length, charSet)
}
