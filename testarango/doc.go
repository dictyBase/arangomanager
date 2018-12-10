/*
Package testarango is a golang library to write unit tests for
arangodb based packages and applications.
The library allows to use existing arangodb instance to create a
isolated and disposable database and users to run unit tests.
The disposable database and users are also cleaned up at the end
of the test cycle.

Prerequisites

	* An existing and running instance of arangodb
	* An existing user with administrative rights to create any
	  random database. The root user could also be used, however
	  care must be taken.

Usage

	A testable instance of arangodb can be obtained in two way,
	either by using env variables or directly supplying the
	credentials.

	* Env variables: Set the following ones
	   ARANGO_HOST
	   ARANGO_USER
	   ARANGO_PASS
	   ARANGO_PORT is optional, by default it uses 8529

		ta,err := NewTestArangoFromEnv(true)

	* Supply them directly

		ta,err := NewTestArango(user,pass,host,port,true)

	Quick Start

			package quickstart

			import (
				"github.com/dictyBase/arangomanager/testarango"
				"testing"
				"log"
				"os"
			)

			var ta *testarango.TestArango

			func TestMain(m *testing.M) {
				ta, err = testarango.NewTestArangoFromEnv(true)
				if err != nil {
					log.Fatal(err)
				}
				code := m.Run()
				// clean up the database at the end
				dbh.Drop()
				os.Exit(code)
			}

			func TestAlmighty(t *testing.T) {
				// connect to disposable test instatnce
				instance, err := ConnectSomeHow(
					ta.User,
					ta.Pass,
					ta.Host,
					ta.Port,
					ta.Database,
					ta.IsTls,
				)
				// run tests here
				.......
			}
*/
package testarango
