package cqlbuilder

import (
	"errors"
	cql "github.com/gocql/gocql"
)

var (
	ErrPreparingQueryFailed = errors.New("cqlbuilder: failed to prepare query")
)

// The execution interface define the method we support to exec cql.
// By this interface we can easily mock CQL for UT purpose.
type ExecManager interface {
	// Exec the single statement
	Exec(c CqlBuilder) error

	// Exec the batch
	ExecBatch(b *BatchBuilder) error

	//Exec query CAS
	ExecCAS(c CqlBuilder, dest ...interface{}) (bool, error)

	//Batch CAS
	ExecBatchCAS(b *BatchBuilder, dest ...interface{}) (applied bool, iter *cql.Iter, err error)

	// run a query and fill back the result.
	ExecScan(c CqlBuilder, dest ...interface{}) error

	// Return result set.
	Iter(c CqlBuilder, des ...interface{}) (*cql.Iter, error)
}

type SessionExecManager struct {
	Session *cql.Session
}

// Exec the single statement
func (em *SessionExecManager) Exec(c CqlBuilder) error {
	return Exec(c, em.Session)
}

// Exec the batch
func (em *SessionExecManager) ExecBatch(b *BatchBuilder) error {
	return ExecBatch(b, em.Session)
}

//Exec query CAS
func (em *SessionExecManager) ExecCAS(c CqlBuilder, dest ...interface{}) (bool, error) {
	return ExecCAS(c, em.Session, dest...)
}

//Batch CAS
func (em *SessionExecManager) ExecBatchCAS(b *BatchBuilder, dest ...interface{}) (applied bool, iter *cql.Iter, err error) {
	return ExecBatchCAS(b, em.Session, dest...)
}

// run a query and fill back the result.
func (em *SessionExecManager) ExecScan(c CqlBuilder, dest ...interface{}) error {
	return ExecScan(c, em.Session, dest...)
}

// Return result set
func (em *SessionExecManager) Iter(c CqlBuilder, des ...interface{}) (*cql.Iter, error){
	cqlstr, vals, err := c.ToQuery()
	if err != nil {
		return nil, err
	}
	q := em.Session.Query(cqlstr, vals...)

	if q != nil {
		return q.Iter(), nil
	}else {
		return nil, ErrPreparingQueryFailed
	}
}