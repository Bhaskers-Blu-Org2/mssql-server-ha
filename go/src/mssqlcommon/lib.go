/*
	Copyright 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

package mssqlcommon

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Diagnostics struct {
	System          bool
	Resource        bool
	QueryProcessing bool
}

type ServerHealth uint

const (
	// The instance is down or refusing connections
	//
	// This library can't distinguish between down or unresponsive, which is why a single health code is used for both,
	// and why there is no enum member with a value of `2`.
	ServerDownOrUnresponsive ServerHealth = 1

	// sp_server_diagnostics detected a critical system error
	ServerCriticalError ServerHealth = 3

	// sp_server_diagnostics detected a moderate resources error
	ServerModerateError ServerHealth = 4

	// sp_server_diagnostics detected an error that's neither moderate nor critical
	ServerAnyQualifiedError ServerHealth = 5
)

type ServerUnhealthyError struct {
	RawValue ServerHealth
	Inner    error
}

func (err *ServerUnhealthyError) Error() string {
	switch err.RawValue {
	case ServerAnyQualifiedError:
		return fmt.Sprintf("AnyQualified %s", err.Inner)

	case ServerModerateError:
		return fmt.Sprintf("Moderate %s", err.Inner)

	case ServerCriticalError:
		return fmt.Sprintf("Critical %s", err.Inner)

	case ServerDownOrUnresponsive:
		return fmt.Sprintf("Unresponsive or down %s", err.Inner)

	default:
		return fmt.Sprintf("Unknown (%d) %s", err.RawValue, err.Inner)
	}
}

type OcfExitCode int

var (
	OCF_ERR_CONFIGURED    OcfExitCode
	OCF_ERR_GENERIC       OcfExitCode
	OCF_ERR_ARGS          OcfExitCode
	OCF_ERR_PERM          OcfExitCode
	OCF_ERR_UNIMPLEMENTED OcfExitCode
	OCF_FAILED_MASTER     OcfExitCode
	OCF_NOT_RUNNING       OcfExitCode
	OCF_RUNNING_MASTER    OcfExitCode
	OCF_SUCCESS           OcfExitCode
)

// --------------------------------------------------------------------------------------
// Function: ImportOcfExitCodes
//
// Description:
//    Imports the OCF exit codes from corresponding environment variables.
//
func ImportOcfExitCodes() error {
	var err error

	OCF_ERR_CONFIGURED, err = importOcfExitCode("OCF_ERR_CONFIGURED")
	if err != nil {
		return err
	}

	OCF_ERR_GENERIC, err = importOcfExitCode("OCF_ERR_GENERIC")
	if err != nil {
		return err
	}

	OCF_ERR_ARGS, err = importOcfExitCode("OCF_ERR_ARGS")
	if err != nil {
		return err
	}

	OCF_ERR_PERM, err = importOcfExitCode("OCF_ERR_PERM")
	if err != nil {
		return err
	}

	OCF_ERR_UNIMPLEMENTED, err = importOcfExitCode("OCF_ERR_UNIMPLEMENTED")
	if err != nil {
		return err
	}

	OCF_FAILED_MASTER, err = importOcfExitCode("OCF_FAILED_MASTER")
	if err != nil {
		return err
	}

	OCF_NOT_RUNNING, err = importOcfExitCode("OCF_NOT_RUNNING")
	if err != nil {
		return err
	}

	OCF_RUNNING_MASTER, err = importOcfExitCode("OCF_RUNNING_MASTER")
	if err != nil {
		return err
	}

	OCF_SUCCESS, err = importOcfExitCode("OCF_SUCCESS")
	if err != nil {
		return err
	}

	return nil
}

func importOcfExitCode(name string) (OcfExitCode, error) {
	stringValue := os.Getenv(name)
	intValue, err := strconv.Atoi(stringValue)
	if err != nil {
		return 0, fmt.Errorf("%s is set to an invalid value [%s]", name, stringValue)
	}

	return OcfExitCode(intValue), nil
}

// --------------------------------------------------------------------------------------
// Function: Diagnose
//
// Description:
//    Uses the server health diagnostics to determine server health
//
// Params:
//    diagnostics: The diagnostics object returned by `QueryDiagnostics()`
//
func Diagnose(diagnostics Diagnostics) error {
	if !diagnostics.System {
		return &ServerUnhealthyError{RawValue: ServerCriticalError, Inner: fmt.Errorf("sp_server_diagnostics result indicates system error")}
	}

	if !diagnostics.Resource {
		return &ServerUnhealthyError{RawValue: ServerModerateError, Inner: fmt.Errorf("sp_server_diagnostics result indicates resource error")}
	}

	if !diagnostics.QueryProcessing {
		return &ServerUnhealthyError{RawValue: ServerAnyQualifiedError, Inner: fmt.Errorf("sp_server_diagnostics result indicates query processing error")}
	}

	return nil
}

// Function: Exit
//
// Description:
//    Helper to exit with the given exit code and error.
//
func Exit(logger *log.Logger, exitCode int, err error) error {
	if err != nil {
		// Print each line individually to ensure that each line is prefixed with the logger prefix
		for _, line := range strings.Split(err.Error(), "\n") {
			logger.Println(line)
		}
	}

	os.Exit(exitCode)

	return nil
}

// --------------------------------------------------------------------------------------
// Function: GetLocalServerName
//
// Description:
//    Gets the local server name.
//
// Params:
//    db: A connection to a SQL Server instance.
//
// Returns:
//    The name of the local server.
//
func GetLocalServerName(db *sql.DB) (serverName string, err error) {
	err = db.QueryRow("SELECT @@SERVERNAME").Scan(&serverName)
	return
}

// Function: OcfExit
//
// Description:
//    Helper to exit with the given OCF exit code and error.
//
//    To distinguish OCF exit codes from other exit codes (like 1 for panics),
//    the actual exit code is 10 + the OCF exit code.
//
func OcfExit(logger *log.Logger, ocfExitCode OcfExitCode, err error) error {
	return Exit(logger, int(ocfExitCode)+10, err)
}

// --------------------------------------------------------------------------------------
// Function: OpenDB
//
// Description:
//    Opens a connection to a SQL Server instance using the given parameters.
//
// Params:
//    hostname: Hostname of the instance.
//    port: Port number for the T-SQL endpoint of the instance.
//    username: Username to use to connect to the instance.
//    password: Password to use to connect to the instance.
//    applicationName: The application name that the connection will use.
//    connectionTimeout: Connection timeout.
//
// Returns:
//    A connection to the SQL Server instance.
//
func OpenDB(hostname string, port uint64, username string, password string, applicationName string, connectionTimeout time.Duration) (*sql.DB, error) {
	query := url.Values{}
	query.Add("app name", applicationName)
	query.Add("connection timeout", fmt.Sprintf("%d", connectionTimeout/time.Second))

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(username, password),
		Host:     fmt.Sprintf("%s:%d", hostname, port),
		RawQuery: query.Encode(),
	}

	connectionString := u.String()

	db, err := sql.Open("mssql", connectionString)
	if err != nil {
		return nil, &ServerUnhealthyError{RawValue: ServerDownOrUnresponsive, Inner: err}
	}

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, &ServerUnhealthyError{RawValue: ServerDownOrUnresponsive, Inner: err}
	}

	return db, nil
}

// --------------------------------------------------------------------------------------
// Function: OpenDBWithHealthCheck
//
// Description:
//    Opens a connection to a SQL Server instance using the given parameters,
//    and performs a health check.
//
// Params:
//    hostname: Hostname of the instance.
//    port: Port number for the T-SQL endpoint of the instance.
//    username: Username to use to connect to the instance.
//    password: Password to use to connect to the instance.
//    connectionTimeout: Connection timeout.
//        If connection fails, this function will retry until this time has elapsed.
//        If this time elapses, the last error encountered will be returned.
//
// Returns:
//    A connection to the SQL Server instance.
//
func OpenDBWithHealthCheck(
	hostname string, port uint64,
	username string, password string,
	applicationName string,
	connectionTimeout time.Duration,
	stdout *log.Logger) (db *sql.DB, err error) {

	dbChannel := make(chan *sql.DB)
	errChannel := make(chan error)
	timeoutChannel := time.After(connectionTimeout)

	go func() {
		var db *sql.DB
		var err error

		for i := uint(1); ; i++ {
			stdout.Printf("Attempt %d to connect to the instance at %s:%d and run sp_server_diagnostics\n", i, hostname, port)

			if db != nil {
				_ = db.Close()
			}

			db, err = OpenDB(hostname, port, username, password, applicationName, connectionTimeout)
			if err == nil {
				stdout.Printf("Connected to the instance at %s:%d\n", hostname, port)
				dbChannel <- db
				return
			}

			stdout.Printf("Attempt %d returned error: %s\n", i, err)

			errChannel <- err

			time.Sleep(1 * time.Second)
		}
	}()

	// Loop until success or timeout
	for {
		select {
		case db = <-dbChannel:
			var diagnostics Diagnostics
			diagnostics, err = QueryDiagnostics(db)
			if err != nil {
				_ = db.Close()
				return nil, err
			}
			err = Diagnose(diagnostics)
			return

		case err = <-errChannel:
			// Store the latest error so that it can be returned on timeout

		case _ = <-timeoutChannel:
			if err == nil {
				// Connection goroutine timed out without failing even once, so construct a ServerDownOrUnresponsive error to return to the caller

				err = &ServerUnhealthyError{
					RawValue: ServerDownOrUnresponsive,
					Inner:    fmt.Errorf("timed out while attempting to connect to the instance at %s:%d and run sp_server_diagnostics", hostname, port),
				}
			}

			return
		}
	}
}

// --------------------------------------------------------------------------------------
// Function: QueryDiagnostics
//
// Description:
//    Gets the server health diagnostics of a SQL Server instance.
//
// Params:
//    db: A connection to the SQL Server instance.
//
func QueryDiagnostics(db *sql.DB) (result Diagnostics, err error) {
	rows, err := db.Query("EXEC sp_server_diagnostics")
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var creationTime, componentType, componentName, stateDesc, data string
		var state int // https://msdn.microsoft.com/en-us/library/ff878233.aspx

		err = rows.Scan(&creationTime, &componentType, &componentName, &state, &stateDesc, &data)
		if err != nil {
			break
		}

		switch componentName {
		case "system":
			result.System = state == 1
		case "resource":
			result.Resource = state == 1
		case "query_processing":
			result.QueryProcessing = state == 1
		}
	}

	err = rows.Err()

	return
}

// --------------------------------------------------------------------------------------
// Function: ReadCredentialsFile
//
// Description:
//    Reads the specified credentials file to extract a SQL username and password.
//    - The first line contains the username.
//    - The second line contains the password.
//    - Lines are separated by LF.
//    - The second line can end with LF or EOF.
//
func ReadCredentialsFile(filename string) (username string, password string, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if !scanner.Scan() {
		err = fmt.Errorf("Could not read first line to extract username.")
		return
	}
	username = scanner.Text()

	if !scanner.Scan() {
		err = fmt.Errorf("Could not read second line to extract password.")
		return
	}
	password = scanner.Text()

	return
}

// --------------------------------------------------------------------------------------
// Function: SetLocalServerName
//
// Description:
//    Sets the local server name to the given name via sp_dropserver + sp_addserver
//
// Params:
//    db: A connection to a SQL Server instance.
//    serverName: The new name of the local server.
//
func SetLocalServerName(db *sql.DB, serverName string) error {
	var currentServerName string
	err := db.QueryRow(`SELECT name FROM sys.servers WHERE server_id = 0`).Scan(&currentServerName)

	if err == nil && strings.EqualFold(currentServerName, serverName) {
		// Existing sys.servers row already has the specified name
		return nil
	}

	if err != nil && err != sql.ErrNoRows {
		// Unexpected error
		return err
	}

	if err == nil {
		// There is an existing sys.servers row and it has a different name than the specified name. Drop it.
		_, err = db.Exec("EXEC sp_dropserver ?", currentServerName)
		if err != nil {
			return err
		}
	}

	// At this point there is no sys.servers row for a local server. Add a row with the specified name.
	_, err = db.Exec("EXEC sp_addserver ?, local", serverName)

	return err
}

func openDBWithHealthCheckInner(
	hostname string, port uint64,
	username string, password string,
	applicationName string,
	connectionTimeout time.Duration) (db *sql.DB, err error) {

	db, err = OpenDB(hostname, port, username, password, applicationName, connectionTimeout)
	if err != nil {
		return
	}

	diagnostics, err := QueryDiagnostics(db)
	if err != nil {
		return
	}

	err = Diagnose(diagnostics)

	return
}
