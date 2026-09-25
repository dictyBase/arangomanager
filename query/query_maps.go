package query

// AQL boolean logic keywords returned by getLogic.
const (
	logicAnd = "AND"
	logicOr  = "OR"
)

// AQL operators used as keys in the filter operator maps.
const (
	opEqual         = "=="
	opStrictEqual   = "==="
	opNotEqual      = "!="
	opStrictNotEq   = "!=="
	opGreater       = ">"
	opLess          = "<"
	opGreaterEqual  = ">="
	opLessEqual     = "<="
	opMatch         = "=~"
	opNotMatch      = "!~"
	opDateEqual     = "$=="
	opDateGreater   = "$>"
	opDateLess      = "$<"
	opDateGtEqual   = "$>="
	opDateLsEqual   = "$<="
	opArrayEqual    = "@=="
	opArrayMatch    = "@=~"
	opArrayNotMatch = "@!~"
	opArrayNotEqual = "@!="
)

func getLogic(input string) string {
	lmap := map[string]string{",": logicOr, ";": logicAnd}

	return lmap[input]
}

// getOperatorMap returns a mapping of filter operators to AQL operators.
// It includes standard comparison operators, date operators (prefixed with $),
// and array operators (prefixed with @).
func getOperatorMap() map[string]string {
	return map[string]string{
		opEqual:         opEqual,
		opStrictEqual:   opEqual,
		opNotEqual:      opNotEqual,
		opGreater:       opGreater,
		opLess:          opLess,
		opGreaterEqual:  opGreaterEqual,
		opLessEqual:     opLessEqual,
		opMatch:         opMatch,
		opNotMatch:      opNotMatch,
		opDateEqual:     opEqual,
		opDateGreater:   opGreater,
		opDateLess:      opLess,
		opDateGtEqual:   opGreaterEqual,
		opDateLsEqual:   opLessEqual,
		opArrayEqual:    opEqual,
		opArrayMatch:    opMatch,
		opArrayNotMatch: opNotMatch,
		opArrayNotEqual: opNotEqual,
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

// getDateOperatorMap returns a mapping of date-specific operators used in filters to their
// corresponding AQL operators. Date operators are prefixed with $ to distinguish them
// from standard operators. When these operators are used, the value is treated as a date.
func getDateOperatorMap() map[string]string {
	return map[string]string{
		opDateEqual:   opEqual,
		opDateGreater: opGreater,
		opDateLess:    opLess,
		opDateGtEqual: opGreaterEqual,
		opDateLsEqual: opLessEqual,
	}
}

// getArrayOperatorMap returns a mapping of array-specific operators used in filters to their
// corresponding AQL operators. Array operators are prefixed with @ to distinguish them
// from standard operators. When these operators are used, the value is treated as an element
// to be searched for within an array.
func getArrayOperatorMap() map[string]string {
	return map[string]string{
		opArrayEqual:    opEqual,
		opArrayMatch:    opMatch,
		opArrayNotMatch: opNotMatch,
		opArrayNotEqual: opNotEqual,
	}
}
