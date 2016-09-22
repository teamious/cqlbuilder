package cqlbuilder

import (
	"bytes"
	"fmt"
)

type conditionBuilder interface {
	toCondition() (string, []interface{})
}

// Create eq condition builder
func Eq(column string, value interface{}) conditionBuilder {
	return &eqBuilder{
		column: column,
		value:  value,
	}
}

func In(col string, vs interface{}) conditionBuilder {
	return &inBuilder {
		column: col,
		values:vs,
	}
}

// Create exists condition builder.
func Exists() conditionBuilder {
	return &existsBuilder{}
}

// string sth like EXISTS AND version=? AND name=?
// values :  1, "test"
func buildCondition(conditions []conditionBuilder) (string, []interface{}) {
	var condition bytes.Buffer
	values := make([]interface{}, 0, len(conditions))
	for i, c := range conditions {
		clause, v := c.toCondition()
		condition.WriteString(clause)
		// The last condition don't need and
		if i != len(conditions)-1 {
			condition.WriteString(and)
		}
		if len(v) > 0 {
			values = append(values, v...)
		}
	}

	return condition.String(), values
}

type inBuilder struct {
	column string
	values  interface{}
}

func (i *inBuilder) toCondition() (string, []interface{}) {
	c := fmt.Sprintf("%s in ?", i.column)
	return c, []interface{}{i.values}
}


type eqBuilder struct {
	column string
	value  interface{}
}

func (eq *eqBuilder) toCondition() (string, []interface{}) {
	c := fmt.Sprintf("%s=?", eq.column)
	return c, []interface{}{eq.value}
}

// To build EXISTS which used if clause of delete/update.
type existsBuilder struct {
}

func (*existsBuilder) toCondition() (string, []interface{}) {
	return exists, nil
}
