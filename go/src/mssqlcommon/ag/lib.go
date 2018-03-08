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

package ag

import (
	"database/sql"
	"fmt"
	"strings"
)

// An AvailabilityMode represents an AG replica's availability mode.
//
// See the availability_mode field in https://msdn.microsoft.com/en-us/library/ff877883.aspx for details.
type AvailabilityMode byte

const (
	// The replica has ASYNCHRONOUS_COMMIT availability mode
	AmASYNCHRONOUS_COMMIT AvailabilityMode = 0

	// The replica has SYNCHRONOUS_COMMIT availability mode
	AmSYNCHRONOUS_COMMIT AvailabilityMode = 1

	// The replica has CONFIGURATION_ONLY availability mode
	AmCONFIGURATION_ONLY AvailabilityMode = 4
)

// A Role represents an AG replica's role.
//
// See the role field in https://msdn.microsoft.com/en-us/library/ff878537.aspx for details.
type Role byte

const (
	// The replica is in RESOLVING role.
	RoleRESOLVING Role = 0

	// The replica is in PRIMARY role.
	RolePRIMARY Role = 1

	// The replica is in SECONDARY role.
	RoleSECONDARY Role = 2
)

// The seeding mode of an AG replica
//
// See the seeding_mode field in https://msdn.microsoft.com/en-us/library/ff877883.aspx for details
type SeedingMode byte

const (
	// The replica is in automatic seeding mode
	SmAUTOMATIC SeedingMode = 0

	// The replica is in manual seeding mode
	SmMANUAL SeedingMode = 1
)

// --------------------------------------------------------------------------------------
// Function: Drop
//
// Description:
//    Drops the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func Drop(db *sql.DB, agName string) error {
	_, err := db.Exec(fmt.Sprintf("DROP AVAILABILITY GROUP %s", quoteName(agName)))
	return err
}

// --------------------------------------------------------------------------------------
// Function: Failover
//
// Description:
//    Performs a failover of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func Failover(db *sql.DB, agName string) error {
	_, err := db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s FAILOVER", quoteName(agName)))
	return err
}

// --------------------------------------------------------------------------------------
// Function: FailoverWithDataLoss
//
// Description:
//    Forces a failover of the given Availability Group, accepting data loss.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func FailoverWithDataLoss(db *sql.DB, agName string) error {
	_, err := db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s FORCE_FAILOVER_ALLOW_DATA_LOSS", quoteName(agName)))
	return err
}

// --------------------------------------------------------------------------------------
// Function: GetAvailabilityMode
//
// Description:
//    Gets the availability mode of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The numeric value and string name of the availability mode, or an error if the AG was not found.
//
func GetAvailabilityMode(db *sql.DB, agName string) (availabilityMode AvailabilityMode, availabilityModeDesc string, err error) {
	err = db.QueryRow(`
		SELECT ar.availability_mode, ar.availability_mode_desc
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE
			ag.name = ?`, agName).Scan(&availabilityMode, &availabilityModeDesc)

	return
}

// --------------------------------------------------------------------------------------
// Function: GetCurrentReplicaName
//
// Description:
//    Gets the name of the local replica of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetCurrentReplicaName(db *sql.DB, agName string) (currentReplicaName string, err error) {
	err = db.QueryRow(`
		SELECT ar.replica_server_name
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE
			ag.name = ?`, agName).Scan(&currentReplicaName)

	return
}

// --------------------------------------------------------------------------------------
// Function: GetDatabaseStates
//
// Description:
//    Gets a string containing the number of databases that belong to the given Availability Group and are not ONLINE.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetDatabaseStates(db *sql.DB, agName string) (result string, err error) {
	stmt, err := db.Prepare(`
		SELECT d.state, d.state_desc, COUNT(*) FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_database_replica_states drs ON drs.group_id = ag.group_id AND drs.is_local = 1
			INNER JOIN sys.databases d on d.database_id = drs.database_id
		WHERE
			ag.name = ? AND d.state <> 0
		GROUP BY d.state, d.state_desc`)
	if err != nil {
		return
	}
	defer stmt.Close()

	rows, err := stmt.Query(agName)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var state byte
		var stateDesc string
		var numDatabases int
		err = rows.Scan(&state, &stateDesc, &numDatabases)
		if err != nil {
			return
		}

		result += fmt.Sprintf("%d databases are %s, ", numDatabases, stateDesc)
	}

	result = strings.TrimSuffix(result, ", ")

	err = rows.Err()

	return
}

// --------------------------------------------------------------------------------------
// Function: GetDBFailoverMode
//
// Description:
//    Gets the DB_FAILOVER setting of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    `true` means ON, `false` means OFF.
//
func GetDBFailoverMode(db *sql.DB, agName string) (dbFailoverMode bool, err error) {
	err = db.QueryRow(`
		SELECT ag.db_failover
		FROM
			sys.availability_groups ag
		WHERE
			ag.name = ?`, agName).Scan(&dbFailoverMode)

	return
}

// --------------------------------------------------------------------------------------
// Function: GetNumSyncCommitReplicas
//
// Description:
//    Gets the number of SYNCHRONOUS_COMMIT replicas in the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetNumSyncCommitReplicas(db *sql.DB, agName string) (numReplicas uint, err error) {
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM
			sys.availability_replicas ar
			INNER JOIN sys.availability_groups ag ON ar.group_id = ag.group_id
		WHERE ag.name = ? AND ar.availability_mode = ?`, agName, AmSYNCHRONOUS_COMMIT).Scan(&numReplicas)

	return
}

// --------------------------------------------------------------------------------------
// Function: GetPrimaryReplicaName
//
// Description:
//    Gets the name of the primary replica of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetPrimaryReplicaName(db *sql.DB, agName string) (primaryReplicaName string, err error) {
	err = db.QueryRow(`
		SELECT ags.primary_replica
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_group_states ags ON ags.group_id = ag.group_id
		WHERE
			ag.name = ?`, agName).Scan(&primaryReplicaName)

	return
}

// --------------------------------------------------------------------------------------
// Function: GetRole
//
// Description:
//    Gets the role of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The numeric value and name of the role, or an error if the AG was not found.
//
func GetRole(db *sql.DB, agName string) (role Role, roleDesc string, err error) {
	err = db.QueryRow(`
		SELECT ars.role, ars.role_desc
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
		WHERE
			ag.name = ?`, agName).Scan(&role, &roleDesc)

	return
}

// --------------------------------------------------------------------------------------
// Function: GetSeedingMode
//
// Description:
//    Gets the seeding mode of the current replica of the given Availability Group
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The numeric value and string name of the seeding mode, or an error if the AG was not found.
//
func GetSeedingMode(db *sql.DB, agName string) (seedingMode SeedingMode, seedingModeDesc string, err error) {
	err = db.QueryRow(`
		SELECT ar.seeding_mode, ar.seeding_mode_desc
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE
			ag.name = ?`, agName).Scan(&seedingMode, &seedingModeDesc)

	return
}

// --------------------------------------------------------------------------------------
// Function: GetSequenceNumber
//
// Description:
//    Gets the sequence number of the current replica of the given Availability Group
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The sequence number.
//
func GetSequenceNumber(db *sql.DB, agName string) (sequenceNumber int64, err error) {
	err = db.QueryRow(`
		SELECT ag.sequence_number
		FROM
			sys.availability_groups ag
		WHERE
			ag.name = ?`, agName).Scan(&sequenceNumber)

	return
}

// --------------------------------------------------------------------------------------
// Function: GrantCreateAnyDatabase
//
// Description:
//    Grants the given Availability Group's replica the permission to create any databases in the AG that aren't present.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GrantCreateAnyDatabase(db *sql.DB, agName string) (err error) {
	_, err = db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s GRANT CREATE ANY DATABASE", quoteName(agName)))
	return
}

// --------------------------------------------------------------------------------------
// Function: SetRequiredSynchronizedSecondariesToCommit
//
// Description:
//    Sets the value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT on the given Availability Group on the instance.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//    newValue: The new REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT value.
//
func SetRequiredSynchronizedSecondariesToCommit(db *sql.DB, agName string, newValue int32) (err error) {
	_, err = db.Exec(fmt.Sprintf(`
		DECLARE @num_ags INT;
		SELECT @num_ags = COUNT(*) FROM sys.availability_groups WHERE name = ? AND required_synchronized_secondaries_to_commit = ?;
		IF @num_ags = 0
			ALTER AVAILABILITY GROUP %s SET (REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = %d)
		;
	`, quoteName(agName), newValue), agName, newValue)
	return
}

// --------------------------------------------------------------------------------------
// Function: SetRoleToSecondary
//
// Description:
//    Sets the role of the given Availability Group to SECONDARY.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func SetRoleToSecondary(db *sql.DB, agName string) (err error) {
	_, err = db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s SET (ROLE = SECONDARY)", quoteName(agName)))
	return
}

// --------------------------------------------------------------------------------------
// Function: quoteName
//
// Description:
//    Equivalent of QUOTENAME with quote_character = '['.
//
// Params:
//    s: The string to be escaped and wrapped in [].
//
func quoteName(s string) string {
	return fmt.Sprintf("[%s]", strings.Replace(s, "]", "]]", -1))
}
