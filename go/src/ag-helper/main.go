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

package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"

	"mssqlcommon"
	mssqlag "mssqlcommon/ag"
)

func main() {
	stdout := log.New(os.Stdout, "", log.LstdFlags)
	stderr := log.New(os.Stderr, "ERROR: ", log.LstdFlags)
	sequenceNumberOut := log.New(os.Stderr, "SEQUENCE_NUMBER: ", 0)

	err := doMain(stdout, stderr, sequenceNumberOut)
	if err != nil {
		mssqlcommon.Exit(stderr, 1, fmt.Errorf("Unexpected error: %s", err))
	}
}

func doMain(stdout *log.Logger, stderr *log.Logger, sequenceNumberOut *log.Logger) error {
	var (
		hostname             string
		sqlPort              uint64
		agName               string
		credentialsFile      string
		applicationName      string
		rawConnectionTimeout int64
		rawHealthThreshold   uint

		action string

		numRetriesForOnlineDatabases               uint
		skipPreCheck                               bool
		sequenceNumbers                            string
		newMaster                                  string
		requiredSynchronizedSecondariesToCommitArg int
	)

	flag.StringVar(&hostname, "hostname", "localhost", "The hostname of the SQL Server instance to connect to. Default: localhost")
	flag.Uint64Var(&sqlPort, "port", 0, "The port on which the instance is listening for logins.")
	flag.StringVar(&agName, "ag-name", "", "The name of the Availability Group")
	flag.StringVar(&credentialsFile, "credentials-file", "", "The path to the credentials file.")
	flag.StringVar(&applicationName, "application-name", "", "The application name to use for the T-SQL connection.")
	flag.Int64Var(&rawConnectionTimeout, "connection-timeout", 30, "The connection timeout in seconds. "+
		"The application will retry connecting to the instance until this time elapses. Default: 30")
	flag.UintVar(&rawHealthThreshold, "health-threshold", uint(mssqlcommon.ServerCriticalError), "The instance health threshold. Default: 3 (SERVER_CRITICAL_ERROR)")
	flag.UintVar(&numRetriesForOnlineDatabases, "online-databases-retries", 60, "The number of times to try waiting for databases to be ONLINE. Default: 60")

	flag.StringVar(&action, "action", "", `One of --start, --stop, --monitor, --pre-promote, --promote, --demote
	start: Start the replica on this node.
	stop: Stop the replica on this node.
	monitor: Monitor the replica on this node.
	pre-start: Before starting a new clone.
	post-stop: After stopping an existing clone.
	pre-promote: Fetch the sequence number of the replica on this node.
	promote: Promote the replica on this node to master.
	demote: Demote the replica on this node to slave.`)

	flag.BoolVar(&skipPreCheck, "skip-precheck", false, "Promote the replica on this node to master even if its availability mode is ASYNCHRONOUS_COMMIT.")
	flag.StringVar(&sequenceNumbers, "sequence-numbers", "", "The sequence numbers of each replica as stored in the cluster. The value is expected to be in the format returned by attrd_updater -QA")
	flag.StringVar(&newMaster, "new-master", "", "The name of the node that is being promoted.")
	flag.IntVar(&requiredSynchronizedSecondariesToCommitArg, "required-synchronized-secondaries-to-commit", -1, "Explicit value for REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT. If not provided, the value will be derived from the number of SYNCHRONOUS_COMMIT replicas.")

	flag.Parse()

	stdout.Printf(
		"ag-helper invoked with hostname [%s]; port [%d]; ag-name [%s]; credentials-file [%s]; application-name [%s]; connection-timeout [%d]; health-threshold [%d]; action [%s]\n",
		hostname, sqlPort,
		agName,
		credentialsFile,
		applicationName,
		rawConnectionTimeout, rawHealthThreshold,
		action)

	switch action {
	case "start":
		stdout.Printf(
			"ag-helper invoked with online-databases-retries [%d]; required-synchronized-secondaries-to-commit [%d]\n",
			numRetriesForOnlineDatabases, requiredSynchronizedSecondariesToCommitArg)

	case "monitor":
		stdout.Printf(
			"ag-helper invoked with online-databases-retries [%d]; required-synchronized-secondaries-to-commit [%d]\n",
			numRetriesForOnlineDatabases, requiredSynchronizedSecondariesToCommitArg)

	case "pre-start":
		stdout.Printf(
			"ag-helper invoked with required-synchronized-secondaries-to-commit [%d]\n",
			requiredSynchronizedSecondariesToCommitArg)

	case "post-stop":
		stdout.Printf(
			"ag-helper invoked with required-synchronized-secondaries-to-commit [%d]\n",
			requiredSynchronizedSecondariesToCommitArg)

	case "promote":
		stdout.Printf(
			"ag-helper invoked with skip-precheck [%t]; sequence-numbers [...]; new-master [%s]; required-synchronized-secondaries-to-commit [%d]\n",
			skipPreCheck, newMaster, requiredSynchronizedSecondariesToCommitArg)
	}

	if hostname == "" {
		return errors.New("a valid hostname must be specified using --hostname")
	}

	if sqlPort == 0 {
		return errors.New("a valid port number must be specified using --port")
	}

	if agName == "" {
		return errors.New("a valid AG name must be specified using --ag-name")
	}

	if credentialsFile == "" {
		return errors.New("a valid path to a credentials file must be specified using --credentials-file")
	}

	if applicationName == "" {
		return errors.New("a valid application name must be specified using --application-name")
	}

	if action == "" {
		return errors.New("a valid action must be specified using --action")
	}

	if action == "promote" {
		if newMaster == "" {
			return errors.New("a valid hostname must be specified using --new-master")
		}
	}

	err := mssqlcommon.ImportOcfExitCodes()
	if err != nil {
		return err
	}

	if action == "stop" {
		// This is a no-op since there is no meaning to "stopping" an AG.
		// Don't even try to connect to the DB or perform a health check.

		return mssqlcommon.OcfExit(stderr, mssqlcommon.OCF_SUCCESS, nil)
	}

	connectionTimeout := time.Duration(rawConnectionTimeout) * time.Second
	healthThreshold := mssqlcommon.ServerHealth(rawHealthThreshold)

	var requiredSynchronizedSecondariesToCommit *uint
	if requiredSynchronizedSecondariesToCommitArg != -1 {
		if requiredSynchronizedSecondariesToCommitArg < 0 || requiredSynchronizedSecondariesToCommitArg > math.MaxInt32 {
			return mssqlcommon.OcfExit(stderr, mssqlcommon.OCF_ERR_CONFIGURED, errors.New(
				"--required-synchronized-secondaries-to-commit must be set to a valid integer between 0 and one less than the number of SYNCHRONOUS_COMMIT replicas (both inclusive)"))
		}

		requiredSynchronizedSecondariesToCommitUint := uint(requiredSynchronizedSecondariesToCommitArg)
		requiredSynchronizedSecondariesToCommit = &requiredSynchronizedSecondariesToCommitUint
	}

	sqlUsername, sqlPassword, err := mssqlcommon.ReadCredentialsFile(credentialsFile)
	if err != nil {
		return mssqlcommon.OcfExit(stderr, mssqlcommon.OCF_ERR_ARGS, fmt.Errorf("Could not read credentials file: %s", err))
	}

	db, err := mssqlcommon.OpenDBWithHealthCheck(
		hostname, sqlPort,
		sqlUsername, sqlPassword,
		applicationName,
		connectionTimeout,
		stdout)
	if err != nil {
		switch serverUnhealthyError := err.(type) {
		case *mssqlcommon.ServerUnhealthyError:
			if serverUnhealthyError.RawValue <= healthThreshold {
				return mssqlcommon.OcfExit(stderr, mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf(
					"Instance health status %d is at or below the threshold value of %d",
					serverUnhealthyError.RawValue, healthThreshold))
			}

			stdout.Printf("Instance health status %d is greater than the threshold value of %d\n", serverUnhealthyError.RawValue, healthThreshold)

		default:
			return err
		}
	}
	defer db.Close()

	stdout.Println("Setting session context...")
	_, err = db.Exec(`EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes', @read_only = 1`)
	if err != nil {
		return mssqlcommon.OcfExit(stderr, mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Failed to set session context: %s", err))
	}

	var ocfExitCode mssqlcommon.OcfExitCode

	switch action {
	case "start":
		ocfExitCode, err = start(db, agName, numRetriesForOnlineDatabases, requiredSynchronizedSecondariesToCommit, stdout)

	case "monitor":
		ocfExitCode, err = monitor(db, agName, numRetriesForOnlineDatabases, requiredSynchronizedSecondariesToCommit, stdout)

	case "pre-start":
		ocfExitCode, err = preStart(db, agName, requiredSynchronizedSecondariesToCommit, stdout)

	case "post-stop":
		ocfExitCode, err = postStop(db, agName, requiredSynchronizedSecondariesToCommit, stdout)

	case "pre-promote":
		ocfExitCode, err = prePromote(db, agName, stdout, sequenceNumberOut)

	case "promote":
		ocfExitCode, err = promote(db, agName, sequenceNumbers, newMaster, skipPreCheck, requiredSynchronizedSecondariesToCommit, stdout)

	case "demote":
		ocfExitCode, err = demote(db, agName)

	default:
		return fmt.Errorf("unknown value for --action %s", action)
	}

	return mssqlcommon.OcfExit(stderr, ocfExitCode, err)
}

// Function: start
//
// Description:
//    Implements the OCF "start" action by ensuring the AG replica exists and is in SECONDARY role.
//
// Returns:
//    OCF_SUCCESS: AG replica exists and is in SECONDARY role.
//    OCF_ERR_GENERIC: Propagated from `monitor()`
//
func start(
	db *sql.DB, agName string,
	numRetriesForOnlineDatabases uint,
	requiredSynchronizedSecondariesToCommit *uint,
	stdout *log.Logger) (mssqlcommon.OcfExitCode, error) {

	// Set replica to SECONDARY, ignoring errors.
	// Errors are ignored to handle the rare case where there's only a single replica total in the AG.
	// ALTER AG SET (ROLE = SECONDARY) fails in this case but also promotes the replica to primary.
	//
	// If the AG is unhealthy for any other reason, this will be caught by `monitor()` below.
	_ = mssqlag.SetRoleToSecondary(db, agName)

	// `SET (ROLE = SECONDARY)` DDL returns before role change finishes, so wait till it completes.
	// This is especially important if the previous role was RESOLVING, because monitor() will interpret
	// RESOLVING to return OCF_NOT_RUNNING. We don't want the "start" action to return OCF_NOT_RUNNING
	// since pacemaker treats that as a hard error and won't try to start the resource any more.
	err := waitUntilRoleSatisfies(db, agName, stdout, func(role mssqlag.Role) bool { return role != mssqlag.RoleRESOLVING })
	if err == sql.ErrNoRows {
		return mssqlcommon.OCF_ERR_ARGS, errors.New("sys.availability_groups does not contain a row for the AG. Local replica may not be joined to the AG.")
	}
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Failed while waiting for local replica to be in SECONDARY role: %s", err)
	}

	// Check health to confirm successful startup
	return monitor(db, agName, numRetriesForOnlineDatabases, requiredSynchronizedSecondariesToCommit, stdout)
}

// Function: monitor
//
// Description:
//    Implements the OCF "monitor" action.
//
// Returns:
//    OCF_SUCCESS: AG replica on this instance is in SECONDARY role.
//    OCF_RUNNING_MASTER: AG replica on this instance is in PRIMARY role. If DB_FAILOVER is ON for this AG,
//        then all databases on this replica are ONLINE.
//    OCF_NOT_RUNNING: The AG is not found in sys.availability_groups, or its role is RESOLVING.
//    OCF_ERR_GENERIC: One of the above is not true.
//
func monitor(
	db *sql.DB, agName string,
	numRetriesForOnlineDatabases uint,
	requiredSynchronizedSecondariesToCommit *uint,
	stdout *log.Logger) (mssqlcommon.OcfExitCode, error) {

	stdout.Printf("Querying role of %s on this node...\n", agName)

	role, roleDesc, err := mssqlag.GetRole(db, agName)
	if err == sql.ErrNoRows {
		stdout.Printf("No row found in sys.availability_groups for %s.\n", agName)
		return mssqlcommon.OCF_NOT_RUNNING, nil
	}
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not query replica role: %s", err)
	}

	stdout.Printf("%s is in %s (%d) role.\n", agName, roleDesc, role)

	if role == mssqlag.RolePRIMARY {
		stdout.Printf("Querying DB_FAILOVER setting of %s...\n", agName)

		dbFailoverMode, err := mssqlag.GetDBFailoverMode(db, agName)
		if err != nil {
			return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not query DB_FAILOVER setting: %s", err)
		}

		var dbFailoverModeString string
		if dbFailoverMode {
			dbFailoverModeString = "ON"
		} else {
			dbFailoverModeString = "OFF"
		}

		stdout.Printf("%s has DB_FAILOVER = %s.\n", agName, dbFailoverModeString)

		if dbFailoverMode {
			err = waitForDatabasesToBeOnline(db, agName, numRetriesForOnlineDatabases, stdout)
			if err != nil {
				return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Failed while waiting for databases to be online: %s", err)
			}
		}

		// Update REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT if necessary
		if requiredSynchronizedSecondariesToCommit == nil {
			err = calculateAndSetRequiredSynchronizedSecondariesToCommit(db, agName, stdout)
			if err != nil {
				return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not calculate and set value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT: %s", err)
			}
		} else {
			err = setRequiredSynchronizedSecondariesToCommit(db, agName, *requiredSynchronizedSecondariesToCommit, stdout)
			if err != nil {
				return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not set value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT: %s", err)
			}
		}

		return mssqlcommon.OCF_RUNNING_MASTER, nil
	} else if role == mssqlag.RoleRESOLVING {
		// AG is neither PRIMARY nor SECONDARY, which means it's waiting to be explicitly set to one or the other via start / promote.
		// So tell Pacemaker that the resource is not running.
		return mssqlcommon.OCF_NOT_RUNNING, nil
	}

	return mssqlcommon.OCF_SUCCESS, nil
}

// Function: preStart
//
// Description:
//    Invoked to handle pre-start notifications from the OCF "notify" action.
//
// Returns:
//    OCF_SUCCESS
//    OCF_ERR_GENERIC
//
func preStart(
	db *sql.DB, agName string,
	requiredSynchronizedSecondariesToCommit *uint,
	stdout *log.Logger) (mssqlcommon.OcfExitCode, error) {

	isPrimary, err := isPrimary(db, agName, stdout)
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not check if local replica is in PRIMARY role: %s", err)
	}

	if isPrimary {
		// A replica is going to start. If it's starting because a new replica was added to the AG, then we need to update REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT.
		if requiredSynchronizedSecondariesToCommit == nil {
			err := calculateAndSetRequiredSynchronizedSecondariesToCommit(db, agName, stdout)
			if err != nil {
				return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not calculate and set value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT: %s", err)
			}
		} else {
			err := setRequiredSynchronizedSecondariesToCommit(db, agName, *requiredSynchronizedSecondariesToCommit, stdout)
			if err != nil {
				return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not set value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT: %s", err)
			}
		}
	}

	return mssqlcommon.OCF_SUCCESS, nil
}

// Function: postStop
//
// Description:
//    Invoked to handle post-stop notifications from the OCF "notify" action.
//
// Returns:
//    OCF_SUCCESS
//    OCF_ERR_GENERIC
//
func postStop(
	db *sql.DB, agName string,
	requiredSynchronizedSecondariesToCommit *uint,
	stdout *log.Logger) (mssqlcommon.OcfExitCode, error) {

	isPrimary, err := isPrimary(db, agName, stdout)
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not check if local replica is in PRIMARY role: %s", err)
	}

	if isPrimary {
		// A replica has stopped. If it stopped because a replica was removed from the AG, then we need to update REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT.
		if requiredSynchronizedSecondariesToCommit == nil {
			err := calculateAndSetRequiredSynchronizedSecondariesToCommit(db, agName, stdout)
			if err != nil {
				return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not calculate and set value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT: %s", err)
			}
		} else {
			err := setRequiredSynchronizedSecondariesToCommit(db, agName, *requiredSynchronizedSecondariesToCommit, stdout)
			if err != nil {
				return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not set value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT: %s", err)
			}
		}
	}

	return mssqlcommon.OCF_SUCCESS, nil
}

// Function: prePromote
//
// Description:
//    Invoked to handle pre-promote notifications from the OCF "notify" action.
//
// Returns:
//    OCF_SUCCESS: Sequence number was fetched successfully.
//    OCF_ERR_GENERIC: Could not query sequence number of the AG replica.
//
func prePromote(
	db *sql.DB, agName string,
	stdout *log.Logger, sequenceNumberOut *log.Logger) (mssqlcommon.OcfExitCode, error) {

	stdout.Printf("Querying sequence number of %s on this node...\n", agName)

	availabilityMode, availabilityModeDesc, err := mssqlag.GetAvailabilityMode(db, agName)
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not query availability mode of local replica: %s", err)
	}

	var sequenceNumber int64
	if availabilityMode == mssqlag.AmSYNCHRONOUS_COMMIT || availabilityMode == mssqlag.AmCONFIGURATION_ONLY {
		sequenceNumber, err = mssqlag.GetSequenceNumber(db, agName)
		if err != nil {
			return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not query sequence number of local replica: %s", err)
		}
	} else {
		stdout.Printf("Availability mode of %s on this node is %s (%d).\n", agName, availabilityModeDesc, availabilityMode)
		sequenceNumber = 0
	}

	stdout.Printf("%s has sequence number 0x%016X\n", agName, sequenceNumber)
	sequenceNumberOut.Println(sequenceNumber)

	return mssqlcommon.OCF_SUCCESS, nil
}

// Function: promote
//
// Description:
//    Implements the OCF "promote" action by failing over the AG replica to PRIMARY role.
//
// Returns:
//    OCF_SUCCESS: AG replica is already in PRIMARY role or was successfully failed over to PRIMARY role.
//    OCF_FAILED_MASTER: AG replica could not be failed over to PRIMARY role and is now in unknown state.
//    OCF_ERR_GENERIC: Could not determine initial role of AG replica, or --skip-precheck was not passed and the availability mode is
//        ASYNCHRONOUS_COMMIT or could not be successfully retrieved, or the sequence number of the AG replica is lower than the
//        sequence number of some other replica.
//
func promote(
	db *sql.DB, agName string,
	sequenceNumbers string,
	newMaster string,
	skipPreCheck bool,
	requiredSynchronizedSecondariesToCommit *uint,
	stdout *log.Logger) (mssqlcommon.OcfExitCode, error) {

	isPrimary, err := isPrimary(db, agName, stdout)
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not check if local replica is in PRIMARY role: %s", err)
	}
	if isPrimary {
		return mssqlcommon.OCF_SUCCESS, nil
	}

	if skipPreCheck {
		stdout.Println("Skipping pre-check since --skip-precheck was specified.")
	} else {
		stdout.Printf("Checking availability mode of %s on this node...\n", agName)

		availabilityMode, availabilityModeDesc, err := mssqlag.GetAvailabilityMode(db, agName)
		if err != nil {
			return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not query availability mode of local replica: %s", err)
		}

		if availabilityMode == mssqlag.AmSYNCHRONOUS_COMMIT {
			stdout.Printf("Availability mode of %s on this node is SYNCHRONOUS_COMMIT.\n", agName)
		} else {
			return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf(
				"Local replica has availabilty mode %s (%d), so it cannot be promoted to PRIMARY",
				availabilityModeDesc, availabilityMode)
		}
	}

	stdout.Println("Verifying local replica's sequence number vs all sequence numbers...")

	var maxSequenceNumber int64
	var newMasterSequenceNumber int64
	var numSequenceNumbers uint

	lineRegex := regexp.MustCompile(`^name="[^"]+" host="([^"]+)" value="(\d+)"$`)

	for _, line := range strings.Split(sequenceNumbers, "\n") {
		stdout.Printf("Sequence number line [%s]\n", line)

		match := lineRegex.FindStringSubmatch(line)
		if match == nil {
			stdout.Println("Line does not match expected syntax. Ignoring.")
			continue
		}

		host := match[1]
		value, err := strconv.ParseInt(match[2], 10, 64)
		if err != nil {
			return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not parse sequence number line: %s", err)
		}

		if host == newMaster {
			newMasterSequenceNumber = value
		}

		if value > maxSequenceNumber {
			maxSequenceNumber = value
		}

		numSequenceNumbers++
	}

	stdout.Printf("Max sequence number of all replicas of %s is %d\n", agName, maxSequenceNumber)
	stdout.Printf("Sequence number of %s replica on %s is %d\n", agName, newMaster, newMasterSequenceNumber)
	stdout.Printf("%d sequence numbers were found\n", numSequenceNumbers)

	stdout.Println("Verifying local replica's sequence number vs all sequence numbers...")

	if newMasterSequenceNumber < maxSequenceNumber {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf(
			"Local replica has sequence number %d but max sequence number is %d, so it cannot be promoted",
			newMasterSequenceNumber, maxSequenceNumber)
	}

	if newMasterSequenceNumber == 0 {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Local replica has sequence number %d, so it cannot be promoted", newMasterSequenceNumber)
	}

	stdout.Println("Querying number of SYNCHRONOUS_COMMIT replicas...")

	numSyncCommitReplicas, err := mssqlag.GetNumSyncCommitReplicas(db, agName)
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not query number of SYNCHRONOUS_COMMIT replicas: %s", err)
	}

	stdout.Printf("%s has %d SYNCHRONOUS_COMMIT replicas.\n", agName, numSyncCommitReplicas)

	var requiredSynchronizedSecondariesToCommitValue uint
	if requiredSynchronizedSecondariesToCommit == nil {
		requiredSynchronizedSecondariesToCommitValue = calculateRequiredSynchronizedSecondariesToCommit(numSyncCommitReplicas)
	} else {
		requiredSynchronizedSecondariesToCommitValue = *requiredSynchronizedSecondariesToCommit
	}

	requiredNumSequenceNumbers := numSyncCommitReplicas - requiredSynchronizedSecondariesToCommitValue
	if numSequenceNumbers < requiredNumSequenceNumbers {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf(
			"Expected to receive %d sequence numbers but only received %d. Not enough replicas are online to safely promote the local replica.",
			numSequenceNumbers, requiredNumSequenceNumbers)
	}

	stdout.Printf("Changing role of %s on this node to primary...\n", agName)

	err = mssqlag.Failover(db, agName)
	if err != nil {
		return mssqlcommon.OCF_FAILED_MASTER, fmt.Errorf("Could not promote local replica to PRIMARY role: %s", err)
	}

	// `FAILOVER` DDL returns before role change finishes, so wait till it completes.
	err = waitUntilRoleSatisfies(db, agName, stdout, func(role mssqlag.Role) bool { return role == mssqlag.RolePRIMARY })
	if err != nil {
		return mssqlcommon.OCF_FAILED_MASTER, fmt.Errorf("Failed while waiting for local replica to be in PRIMARY role: %s", err)
	}

	stdout.Printf("%s is now primary role.\n", agName)

	err = setRequiredSynchronizedSecondariesToCommit(db, agName, requiredSynchronizedSecondariesToCommitValue, stdout)
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not set value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT: %s", err)
	}

	return mssqlcommon.OCF_SUCCESS, nil
}

// Function: demote
//
// Description:
//    Implements the OCF "demote" action by setting the AG replica to SECONDARY role.
//
// Returns:
//    OCF_SUCCESS: AG replica was successfully set to SECONDARY role.
//    OCF_ERR_GENERIC: Could not set AG replica to SECONDARY role.
//
func demote(db *sql.DB, agName string) (mssqlcommon.OcfExitCode, error) {
	// Set replica to SECONDARY
	err := mssqlag.SetRoleToSecondary(db, agName)
	if err != nil {
		return mssqlcommon.OCF_ERR_GENERIC, fmt.Errorf("Could not set local replica to SECONDARY role: %s", err)
	}

	return mssqlcommon.OCF_SUCCESS, nil
}

// Function: waitForDatabasesToBeOnline
//
// Description:
//    Waits for all databases in the AG to be ONLINE.
//    Periodically prints a message detailing the number of databases that are not ONLINE.
//
func waitForDatabasesToBeOnline(
	db *sql.DB, agName string,
	numRetriesForOnlineDatabases uint,
	stdout *log.Logger) error {

	var lastErr error

	for i := uint(0); i < numRetriesForOnlineDatabases; i++ {
		nonOnlineDatabasesMessage, err := mssqlag.GetDatabaseStates(db, agName)
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		if len(nonOnlineDatabasesMessage) > 0 {
			stdout.Println(nonOnlineDatabasesMessage)
			lastErr = errors.New(nonOnlineDatabasesMessage)
			time.Sleep(1 * time.Second)
			continue
		}

		// All ready
		stdout.Println("All databases are ONLINE.")
		return nil
	}

	return lastErr
}

func isPrimary(db *sql.DB, agName string, stdout *log.Logger) (result bool, err error) {
	stdout.Printf("Querying role of %s on this node...\n", agName)

	role, roleDesc, err := mssqlag.GetRole(db, agName)
	if err != nil {
		return
	}

	stdout.Printf("%s is in %s (%d) role.\n", agName, roleDesc, role)

	result = role == mssqlag.RolePRIMARY

	return
}

func calculateAndSetRequiredSynchronizedSecondariesToCommit(db *sql.DB, agName string, stdout *log.Logger) (err error) {
	stdout.Println("Querying number of SYNCHRONOUS_COMMIT replicas...")

	numSyncCommitReplicas, err := mssqlag.GetNumSyncCommitReplicas(db, agName)
	if err != nil {
		return
	}

	stdout.Printf("%s has %d SYNCHRONOUS_COMMIT replicas.\n", agName, numSyncCommitReplicas)

	calculatedRequiredSynchronizedSecondariesToCommit := calculateRequiredSynchronizedSecondariesToCommit(numSyncCommitReplicas)

	err = setRequiredSynchronizedSecondariesToCommit(db, agName, calculatedRequiredSynchronizedSecondariesToCommit, stdout)

	return
}

func calculateRequiredSynchronizedSecondariesToCommit(numReplicas uint) uint {
	// quorum count = (numReplicas / 2) + 1
	// required synchronized secondaries to commit = quorum count - 1 (value doesn't count the primary)
	//
	// Configuration-only replicas are not counted as synchronized secondaries since RSSTC accounts for them internally.
	//
	// But for two replicas, (P + S / P + S + C), customers prefer RSSTC = 0 since they don't want unavailablility on the single S to block writes on P

	if numReplicas == 2 {
		return 0
	}

	return numReplicas / 2
}

func setRequiredSynchronizedSecondariesToCommit(
	db *sql.DB, agName string,
	requiredSynchronizedSecondariesToCommit uint,
	stdout *log.Logger) (err error) {

	stdout.Printf("Setting REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT of %s to %d...\n", agName, requiredSynchronizedSecondariesToCommit)

	err = mssqlag.SetRequiredSynchronizedSecondariesToCommit(db, agName, int32(requiredSynchronizedSecondariesToCommit))

	return
}

func waitUntilRoleSatisfies(db *sql.DB, agName string, stdout *log.Logger, predicate func(mssqlag.Role) bool) error {
	for {
		stdout.Printf("Querying role of %s on this node...\n", agName)

		role, roleDesc, err := mssqlag.GetRole(db, agName)
		if err != nil {
			return err
		}

		stdout.Printf("%s is in %s (%d) role.\n", agName, roleDesc, role)

		if predicate(role) {
			return nil
		}
	}
}
