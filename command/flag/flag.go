package flag

import (
	"github.com/urfave/cli"
)

/*
The ArangoFlags function returns a []cli.Flag containing the following flags:

  - arangodb-pass: The password for the ArangoDB database.

    This flag is required and can be set using the ARANGODB_PASS environment variable.

  - arangodb-user: The user for the ArangoDB database.

    This flag is required and can be set using the ARANGODB_USER environment variable.

  - arangodb-host: The host for the ArangoDB database.

    The default value is "arangodb" and can be overridden using the ARANGODB_SERVICE_HOST
    environment variable. This flag is required.

  - arangodb-port: The port for the ArangoDB database.

    The default value is "8529" and can be overridden using the ARANGODB_SERVICE_PORT environment variable.

  - is-secure: A boolean flag indicating whether the ArangoDB endpoint is secured or unsecured.

Example usage:

	flags := ArangoFlags()
	app := cli.NewApp()
	app.Flags = flags
	...
	err := app.Run(os.Args)

The ArangoFlags function can be used in a command-line application to easily configure ArangoDB connection details.
*/
func ArangoFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:     "arangodb-pass, pass",
			EnvVar:   "ARANGODB_PASS",
			Usage:    "arangodb database password",
			Required: true,
		},
		cli.StringFlag{
			Name:     "arangodb-user, user",
			EnvVar:   "ARANGODB_USER",
			Usage:    "arangodb database user",
			Required: true,
		},
		cli.StringFlag{
			Name:     "arangodb-host, host",
			Value:    "arangodb",
			EnvVar:   "ARANGODB_SERVICE_HOST",
			Usage:    "arangodb database host",
			Required: true,
		},
		cli.StringFlag{
			Name:   "arangodb-port",
			EnvVar: "ARANGODB_SERVICE_PORT",
			Usage:  "arangodb database port",
			Value:  "8529",
		},
		cli.BoolFlag{
			Name:  "is-secure",
			Usage: "flag for secured or unsecured arangodb endpoint",
		},
	}
}

// ArangodbFlags returns the cli based flag slice that includes
// command line arguments for connecting to an arangodb instance.
func ArangodbFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:     "arangodb-pass, pass",
			EnvVar:   "ARANGODB_PASS",
			Usage:    "arangodb database password",
			Required: true,
		},
		cli.StringFlag{
			Name:     "arangodb-database, db",
			EnvVar:   "ARANGODB_DATABASE",
			Usage:    "arangodb database name",
			Required: true,
		},
		cli.StringFlag{
			Name:     "arangodb-user, user",
			EnvVar:   "ARANGODB_USER",
			Usage:    "arangodb database user",
			Required: true,
		},
		cli.StringFlag{
			Name:     "arangodb-host, host",
			Value:    "arangodb",
			EnvVar:   "ARANGODB_SERVICE_HOST",
			Usage:    "arangodb database host",
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
