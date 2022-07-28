package query

var arrToAQL = map[string]string{
	"==": "IN",
	"!=": "NOT IN",
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
