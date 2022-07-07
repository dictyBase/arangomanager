package flag

import (
	"github.com/urfave/cli"
)

// ArangodbFlags returns the cli based flag slice that includes
// command line arguments for connecting to an arangodb instance.
func ArangodbFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:   "arangodb-pass, pass",
			EnvVar: "ARANGODB_PASS",
			Usage:  "arangodb database password",
			Required: true,
		},
		cli.StringFlag{
			Name:   "arangodb-database, db",
			EnvVar: "ARANGODB_DATABASE",
			Usage:  "arangodb database name",
			Required: true,
		},
		cli.StringFlag{
			Name:   "arangodb-user, user",
			EnvVar: "ARANGODB_USER",
			Usage:  "arangodb database user",
			Required: true,
		},
		cli.StringFlag{
			Name:   "arangodb-host, host",
			Value:  "arangodb",
			EnvVar: "ARANGODB_SERVICE_HOST",
			Usage:  "arangodb database host",
			Required: true,
		},
		cli.StringFlag{
			Name:   "arangodb-port",
			EnvVar: "ARANGODB_SERVICE_PORT",
			Usage:  "arangodb database port",
			Value:  "8529",
		},
		cli.BoolTFlag{
			Name:  "is-secure",
			Usage: "flag for secured or unsecured arangodb endpoint",
		},
	}
}
