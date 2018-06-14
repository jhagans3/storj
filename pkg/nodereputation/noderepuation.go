// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package nodereputation

import (
	"bytes"
	"context"
	"database/sql"

	// import of sqlite3 for side effects
	_ "github.com/mattn/go-sqlite3"

	proto "storj.io/storj/protos/nodereputation"
	sqlite "storj.io/storj/storage/sqlite3"
)

// NodeRepDB the methods for a data store that stores reputation data
type NodeRepDB interface {
	GetRows(ctx context.Context, constraints []proto.NodeReputationConstraint, limit int) ([]proto.NodeReputationRecord, error)
	Lookup(ctx context.Context, nodeID proto.NodeID) (proto.NodeReputationRecord, error)
	UpdateRows(ctx context.Context, rows []proto.NodeReputationRecord) error
}

// func NewNodeRepDB() NodeRepDB {

// }

// auditSuccessRatio finds the ratio of audit success from the success and failure fields of a given reputaion row struct
func auditSuccessRatio(row proto.NodeReputationRecord) float32 {
	res := float32(0)
	total := row.auditSuccess + row.auditFail

	if total > 0 {
		res = float32(row.auditSuccess) / float32(total)
	}
	return res
}

/*
  endian method hot encodes the two nodeReputationRecord structs
  desired values are set to a one, other values are set to zeros
  then compares and returns the largest
  order of evaluation is ordered from most significant column (first)
  to the least significant column (last position in the slice)
*/
func endian(row proto.NodeReputationRecord, other proto.NodeReputationRecord, orderOfEval []sqlite.Column) proto.NodeReputationRecord {
	var rowEndian bytes.Buffer
	var otherEndian bytes.Buffer

	for _, order := range orderOfEval {
		switch order {
		case timestampColumn:
			switch {
			case row.timestamp > other.timestamp && row.nodeName == other.nodeName:
				rowEndian.WriteString("1")
				otherEndian.WriteString("0")
			case row.timestamp < other.timestamp && row.nodeName == other.nodeName:
				rowEndian.WriteString("0")
				otherEndian.WriteString("1")
			default:
				rowEndian.WriteString("0")
				otherEndian.WriteString("0")
			}

		case shardsModifiedColumn:
			if row.shardsModified > 0 {
				rowEndian.WriteString("0")
			} else {
				rowEndian.WriteString("1")
			}
			if other.shardsModified > 0 {
				otherEndian.WriteString("0")
			} else {
				otherEndian.WriteString("1")
			}

		case falseClaimsColumn:
			switch {
			case row.falseClaims < other.falseClaims:
				rowEndian.WriteString("1")
				otherEndian.WriteString("0")
			case row.falseClaims > other.falseClaims:
				rowEndian.WriteString("0")
				otherEndian.WriteString("1")
			default:
				rowEndian.WriteString("0")
				otherEndian.WriteString("0")
			}

		case auditSuccessColumn:
			switch {
			case row.auditSuccessRatio() > other.auditSuccessRatio():
				rowEndian.WriteString("1")
				otherEndian.WriteString("0")
			case row.auditSuccessRatio() < other.auditSuccessRatio():
				rowEndian.WriteString("0")
				otherEndian.WriteString("1")
			default:
				rowEndian.WriteString("0")
				otherEndian.WriteString("0")
			}

		case uptimeColumn:
			switch {
			case row.uptime > other.uptime:
				rowEndian.WriteString("1")
				otherEndian.WriteString("0")
			case row.uptime < other.uptime:
				rowEndian.WriteString("0")
				otherEndian.WriteString("1")
			default:
				rowEndian.WriteString("0")
				otherEndian.WriteString("0")
			}

		case latencyColumn:
			switch {
			case row.latency < other.latency:
				rowEndian.WriteString("1")
				otherEndian.WriteString("0")
			case row.latency > other.latency:
				rowEndian.WriteString("0")
				otherEndian.WriteString("1")
			default:
				rowEndian.WriteString("0")
				otherEndian.WriteString("0")
			}

		case amountOfDataStoredColumn:
			switch {
			case row.amountOfDataStored > other.amountOfDataStored:
				rowEndian.WriteString("1")
				otherEndian.WriteString("0")
			case row.amountOfDataStored < other.amountOfDataStored:
				rowEndian.WriteString("0")
				otherEndian.WriteString("1")
			default:
				rowEndian.WriteString("0")
				otherEndian.WriteString("0")
			}
		}
	}

	var res nodeReputationRecord

	if rowEndian.String() > otherEndian.String() {
		res = row
	} else {
		res = other
	}

	return res
}

/*
  endianReputation based on the most significant fields of nodeReputationRecord
  order is as follows:
  timestamp, most recent values of rows with the same name equals a one
  shardsModified, if any value other than zero is found a zero is needed
  falseClaims, more false claims equal a zero
  auditSuccessRatio, higher ratio equals a one
  uptime, higher uptime equals a one
  latency, lower latency equals a one
  amountOfDataStored, more data equals a one
*/
func endianReputation(db *sql.DB, queryString string) (proto.NodeReputationRecord, error) {
	bestRep := newReputationRow("self", "identity")

	rows, err := db.Query(queryString)
	if err != nil {
		return bestRep, SelectError.Wrap(err)
	}
	defer rows.Close()

	transformedRows, err := iterOnDBRows(rows)
	if err != nil {
		return bestRep, SelectError.Wrap(err)
	}

	order := []column{
		timestampColumn,
		shardsModifiedColumn,
		falseClaimsColumn,
		auditSuccessColumn,
		uptimeColumn,
		latencyColumn,
		amountOfDataStoredColumn,
	}

	for _, row := range transformedRows {
		bestRep = bestRep.endian(row, order)
	}

	err = rows.Err()
	if err != nil {
		return bestRep, SelectError.Wrap(err)
	}

	return bestRep, nil
}

type mutOp string

// coproduct/sum type for the mutation operation type
const (
	increment mutOp = "increment"
	decrement mutOp = "decrement"
	overWrite mutOp = "overWrite"
)

// performOp performs an operation that is passed to it on a value with the scalar
func performOp(op mutOp, value int64, scalar int64) int64 {
	switch op {
	case increment:
		value = value + scalar
	case decrement:
		value = value - scalar
	case overWrite:
		value = scalar
	}

	return value
}

// morphism is the name of this function becuse it does not directly change the nodeReputationRecord (more map/functor like)
func morphism(row proto.NodeReputationRecord, col sqlite.Column, op mutOp, scalar int64) proto.NodeReputationRecord {
	switch col {
	case uptimeColumn:
		row.uptime = performOp(op, row.uptime, scalar)
	case auditSuccessColumn:
		row.auditSuccess = performOp(op, row.auditSuccess, scalar)
	case auditFailColumn:
		row.auditFail = performOp(op, row.auditFail, scalar)
	case latencyColumn:
		row.latency = performOp(op, row.latency, scalar)
	case amountOfDataStoredColumn:
		row.amountOfDataStored = performOp(op, row.amountOfDataStored, scalar)
	case falseClaimsColumn:
		row.falseClaims = performOp(op, row.falseClaims, scalar)
	case shardsModifiedColumn:
		row.shardsModified = performOp(op, row.shardsModified, scalar)
	}

	return row
}

// newReputationRow this is the apply function for the reputation row struct, returns a new nodeReputationRecord
func newReputationRow(source string, name string) proto.NodeReputationRecord {
	return nodeReputationRecord{source, name, "", 0, 0, 0, 0, 0, 0, 0}
}
