package cqlbuilder

import (
	"errors"

	cql "github.com/gocql/gocql"
)

// Key words and const bytes for building Cql.
const (
	comma       = ","
	leftPar     = "("
	rightPar    = ")"
	questMak    = "?"
	insert      = " INSERT INTO "
	valuesSt    = " VALUES"
	space       = " "
	update      = " UPDATE "
	set         = " SET "
	ifs         = " IF "
	eq          = " =? "
	where       = " WHERE "
	and         = " AND "
	using       = " USING "
	ttl         = " TTL ? "
	ifNotExists = " IF NOT EXISTS "
	exists      = " EXISTS "
	deleteKW    = " DELETE "
	from        = " FROM "
	selectKW    = " SELECT "
	limit       = " LIMIT "

	allowFiltering = " ALLOW FILTERING "
)

var (
	errEmptyColumn    = errors.New("Need at least one column/values pair")
	errEmptyTable     = errors.New("Need table name")
	errEmptyCondition = errors.New("Need at least one condition")
)

// the interface define for batch operation.
type CqlBuilder interface {
	ToQuery() (string, []interface{}, error)
}

//Create insert builder
func Insert(t string) *InsertBuilder {
	ret := InsertBuilder{
		table: t,
	}
	return &ret
}

//Create update builder
func Update(t string) *UpdateBuilder {
	ret := UpdateBuilder{
		table: t,
	}
	return &ret
}

func Select(t string) *SelectBuilder {
	ret := SelectBuilder{
		table: t,
	}
	return &ret
}

func Delete(t string) *DeleteBuilder {
	ret := DeleteBuilder{
		table: t,
	}
	return &ret
}

func StartBatch() *BatchBuilder {
	ret := BatchBuilder{}

	return &ret
}

// Exec the single statement
func Exec(c CqlBuilder, session *cql.Session) error {
	str, vals, err := c.ToQuery()

	if err != nil {
		return err
	}

	err = session.Query(str, vals...).Exec()
	return err
}

// Exec the batch
func ExecBatch(b *BatchBuilder, session *cql.Session) error {
	batch := session.NewBatch(cql.LoggedBatch)
	for _, q := range b.builders {
		str, vals, err := q.ToQuery()
		if err != nil {
			return err
		}

		batch.Query(str, vals...)
	}

	err := session.ExecuteBatch(batch)
	return err
}

//Exec query CAS
func ExecCAS(c CqlBuilder, s *cql.Session, dest ...interface{}) (bool, error) {

	cqlstr, vals, err := c.ToQuery()
	if err != nil {
		return false, err
	}
	q := s.Query(cqlstr, vals...)
	applied, err := q.ScanCAS(dest...)

	return applied, err
}

// Return result set
func Iter(c CqlBuilder, s *cql.Session, des ...interface{}) (*cql.Iter, error) {
	cqlstr, vals, err := c.ToQuery()
	if err != nil {
		return nil, err
	}
	q := s.Query(cqlstr, vals...)

	return q.Iter(), nil
}

//Batch CAS
func ExecBatchCAS(b *BatchBuilder, session *cql.Session, dest ...interface{}) (applied bool, iter *cql.Iter, err error) {
	batch := session.NewBatch(cql.LoggedBatch)
	for _, q := range b.builders {
		str, vals, err := q.ToQuery()
		if err != nil {
			return false, nil, err
		}

		batch.Query(str, vals...)
	}

	applied, iter, err = session.ExecuteBatchCAS(batch, dest...)

	return applied, iter, err
}

// run a query and fill back the result.
func ExecScan(c CqlBuilder, s *cql.Session, dest ...interface{}) error {
	cqlstr, vals, err := c.ToQuery()
	if err != nil {
		return err
	}

	q := s.Query(cqlstr, vals...)

	err = q.Scan(dest...)

	return err
}
