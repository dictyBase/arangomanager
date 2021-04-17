package arangomanager

const (
	truncateFn = `
		function(params) {
			const db = require('@arangodb').db
			params[0]
				.map(name => db._collection(name))
				.map(collection => collection.truncate())
		}
`
)
