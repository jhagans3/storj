// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package nodereputation

import (
	"database/sql"
	"os"
	"strconv"

	"github.com/zeebo/errs"
	// import of sqlite3 for side effects
	_ "github.com/mattn/go-sqlite3"
)

// StartDBError is an error class for errors related to the reputation package
var StartDBError = errs.Class("reputation start sqlite3 error")

// CreateTableError is an error class for errors related to the reputation package
var CreateTableError = errs.Class("reputation table creation error")

// InsertError is an error class for errors related to the reputation package
var InsertError = errs.Class("reputation insertion error")

// SelectError is an error class for errors related to the reputation package
var SelectError = errs.Class("reputation selection error")

// IterError is an error class for errors related to the reputation package
var IterError = errs.Class("reputation iteration error")

// DeleteError is an error class for errors related to the reputation package
var DeleteError = errs.Class("reputation deletion error")

// nodeReputationRecord is the Data type for Rows in Reputation table
type nodeReputationRecord struct {
	nodeName           string
	timestamp          string
	uptime             int64
	auditSuccess       int64
	auditFail          int64
	latency            int64
	amountOfDataStored int64
	falseClaims        int64
	shardsModified     int64
}

// Column base type for nodeReputationRecord struct
type Column string

// coproduct/sum type for the column type
const (
	nodeNameColumn           Column = "nodeName"
	lastSeenColumn           Column = "lastseen"
	uptimeColumn             Column = "uptime"
	auditSuccessColumn       Column = "auditSuccess"
	auditFailColumn          Column = "auditFail"
	latencyColumn            Column = "latency"
	amountOfDataStoredColumn Column = "amountOfDataStored"
	falseClaimsColumn        Column = "falseClaims"
	shardsModifiedColumn     Column = "shardsModified"
)

// startDB starts a sqlite3 database from the file path parameter
func startDB(filePath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filePath)
	if err != nil {
		return nil, StartDBError.Wrap(err)
	}

	return db, nil
}

// EndServerDB cleans up the passed in database
func EndServerDB(db *sql.DB) error {
	return closeDB(db)
}

// createTable creates a table in sqlite3 based on the create table string parameter
func createTable(db *sql.DB) error {

	var createStmt = `CREATE table node_reputation (
		node_name TEXT NOT NULL,
		last_seen timestamp DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')) NOT NULL,
		uptime INTEGER,
		audit_success INTEGER,
		audit_fail INTEGER,
		latency INTEGER,
		amount_of_data_stored INTEGER,
		false_claims INTEGER,
		shards_modified INTEGER,
	PRIMARY KEY(node_name, last_seen)
	);`

	_, err := db.Exec(createStmt)
	if err != nil {
		return CreateTableError.Wrap(err)
	}
	return nil
}

// SetServerDB public function for a server
func SetServerDB(filepath string) (*sql.DB, error) {

	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		db, err := startDB(filepath)
		if err != nil {
			return nil, err
		}
		err = createTable(db)
		if err != nil {
			return nil, err
		}
		return db, nil
	}
	db, err := startDB(filepath)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// InsertRows inserts the slice of reputation row structs based on the insert string
func InsertRows(db *sql.DB, rows []nodeReputationRecord) error {
	tx, err := db.Begin()
	if err != nil {
		return InsertError.Wrap(err)
	}
	defer tx.Rollback()

	var insertString = `INSERT
	INTO node_reputation (
		node_name,
		uptime,
		audit_success,
		audit_fail,
		latency,
		amount_of_data_stored,
		false_claims,
		shards_modified
	) values (?, ?, ?, ?, ?, ?, ?, ?);`

	insertStmt, err := tx.Prepare(insertString)
	if err != nil {
		return InsertError.Wrap(err)
	}
	defer insertStmt.Close()

	for _, row := range rows {
		_, err = insertStmt.Exec(
			row.nodeName,
			row.uptime,
			row.auditSuccess,
			row.auditFail,
			row.latency,
			row.amountOfDataStored,
			row.falseClaims,
			row.shardsModified,
		)
		if err != nil {
			return InsertError.Wrap(err)
		}
	}
	return tx.Commit()
}

type updateProtoTodo struct {
	col   Column
	value string
}

// GenUpdateStatement is a function that makes a sql string with 1:N updates to a node or a empty select
func GenUpdateStatement(db *sql.DB, updates []updateProtoTodo, node string) string {
	acc := `
UPDATE node_reputation
SET last_seen = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')`

	if len(updates) > 0 {
		for _, update := range updates {
			switch update.col {
			case uptimeColumn:
				acc = acc + ", uptime = " + update.value
			case auditSuccessColumn:
				acc = acc + ", audit_success = " + update.value
			case auditFailColumn:
				acc = acc + ", audit_fail = " + update.value
			case latencyColumn:
				acc = acc + ", latency = " + update.value
			case amountOfDataStoredColumn:
				acc = acc + ", amount_of_data_stored = " + update.value
			case falseClaimsColumn:
				acc = acc + ", false_claims = " + update.value
			case shardsModifiedColumn:
				acc = acc + ", shards_modified = " + update.value
			}
		}

		acc = acc + "WHERE node_name = '" + node + "';"
	} else {
		acc = "SELECT 'No updates found in parameter list';"
	}

	return acc
}

// WhereOpt base type for the filter operation for a sql where clause
type WhereOpt string

// coproduct/sum type for the generation of the sql string statement
const (
	equal        WhereOpt = "="
	greater      WhereOpt = ">"
	greaterEqual WhereOpt = ">="
	less         WhereOpt = "<"
	lessEqual    WhereOpt = "<="
	notEqual     WhereOpt = "!="
)

// toString is a method to convert the sum type to a string for the sql string
func (opt WhereOpt) toString() string {
	res := ""
	switch opt {
	case equal:
		res = "="
	case greater:
		res = ">"
	case greaterEqual:
		res = ">="
	case less:
		res = "<"
	case lessEqual:
		res = "<="
	case notEqual:
		res = "!="
	}

	return res
}

type whereProtoTodo struct {
	col   Column
	opt   WhereOpt
	value string
}

// GenWhereStatement is a function that makes a sql string with many where clauses
func GenWhereStatement(limit uint64, opts []whereProtoTodo) string {
	where := " WHERE"

	for _, opt := range opts {
		operand := opt.opt.toString()
		switch opt.col {
		case nodeNameColumn:
			where = where + " node_name" + operand + " '" + opt.value + "'"
		case lastSeenColumn:
			where = where + " timestamp" + operand + " STRFTIME('%Y-%m-%d %H:%M:%f'," + opt.value + ")"
		case uptimeColumn:
			where = where + " uptime" + operand + " " + opt.value
		case auditSuccessColumn:
			where = where + " audit_succes" + operand + " " + opt.value
		case auditFailColumn:
			where = where + " audit_fail" + operand + " " + opt.value
		case latencyColumn:
			where = where + " latency" + operand + " " + opt.value
		case amountOfDataStoredColumn:
			where = where + " amount_of_data_stored" + operand + " " + opt.value
		case falseClaimsColumn:
			where = where + " false_claims" + operand + " " + opt.value
		case shardsModifiedColumn:
			where = where + " shards_modified" + operand + " " + opt.value

		default:
			where = ""
		}
	}

	var selectString = `SELECT
	node_name,
	timestamp,
	uptime,
	audit_success,
	audit_fail,
	latency,
	amount_of_data_stored,
	false_claims,
	shards_modified
FROM node_reputation`

	return selectString + where + ` LIMIT ` + strconv.FormatUint(limit, 10)
}

// iterOnDBRows iterate on rows in the database to transform into slice of nodeReputationRecord
func iterOnDBRows(rows *sql.Rows) ([]nodeReputationRecord, error) {
	var res []nodeReputationRecord

	for rows.Next() {
		var row nodeReputationRecord

		err := rows.Scan(
			&row.nodeName,
			&row.timestamp,
			&row.uptime,
			&row.auditSuccess,
			&row.auditFail,
			&row.latency,
			&row.amountOfDataStored,
			&row.falseClaims,
			&row.shardsModified,
		)
		if err != nil {
			return nil, IterError.Wrap(err)
		}

		res = append(res, row)
	}

	return res, nil
}

// getNodeReputationRecords function that returns a slice of reputation rows based on the query string
func getNodeReputationRecords(db *sql.DB) ([]nodeReputationRecord, error) {

	var selectString = `SELECT
	node_name,
	timestamp,
	uptime,
	audit_success,
	audit_fail,
	latency,
	amount_of_data_stored,
	false_claims,
	shards_modified
FROM node_reputation`

	rows, err := db.Query(selectString)
	if err != nil {
		return nil, SelectError.Wrap(err)
	}
	defer rows.Close()

	res, err := iterOnDBRows(rows)
	if err != nil {
		return nil, SelectError.Wrap(err)
	}

	err = rows.Err()
	if err != nil {
		return nil, SelectError.Wrap(err)
	}

	return res, nil
}

// closeDB close sqlite3
func closeDB(db *sql.DB) error {
	return db.Close()
}
