package cqlbuilder

import (
	"bytes"
	"errors"
	"strconv"
)

// The select builder.
type SelectBuilder struct {
	colums          []string
	whereConditions []conditionBuilder
	limitNumber     int
	table           string
	allowFiltering  bool
}

// Set a value
// If the value already been set, new value will overwrite old one.
func (c *SelectBuilder) AddColumn(name string) *SelectBuilder {
	c.colums = append(c.colums, name)
	return c
}

func (c *SelectBuilder) AddColumns(cols ...string) *SelectBuilder {
	for _, col := range cols {
		c.colums = append(c.colums, col)
	}
	return c
}

// Set the where condition.
func (c *SelectBuilder) Where(condition conditionBuilder) *SelectBuilder {
	c.whereConditions = append(c.whereConditions, condition)

	return c
}

//Set limit
func (c *SelectBuilder) SetLimit(n int) *SelectBuilder {
	c.limitNumber = n
	return c
}

//Set allow filtering
func (c *SelectBuilder) SetAllowFiltering(allow bool) *SelectBuilder {
	c.allowFiltering = allow
	return c
}

// Validate
func (c *SelectBuilder) validate() error {
	if len(c.table) == 0 {
		return errEmptyTable
	}

	if len(c.colums) == 0 {
		return errEmptyColumn
	}

	if len(c.whereConditions) == 0 {
		return errEmptyCondition
	}

	return nil
}

// Build the select query statement string and values.
// Example:
//  SELECT col1,col2,Col3 FROM test WHERE Col4=? AND Col5=?
func (c *SelectBuilder) ToQuery() (string, []interface{}, error) {
	if err := c.validate(); err != nil {
		return "", nil, err
	}

	var buf bytes.Buffer

	buf.WriteString(selectKW)

	for i := 0; i < len(c.colums); i++ {
		if len(c.colums[i]) == 0 {
			return "", nil, errors.New("Column name can't be nil")
		}

		if i > 0 {
			buf.WriteString(comma)
		}

		buf.WriteString(c.colums[i])
	}

	buf.WriteString(from)
	buf.WriteString(c.table)

	condition, conditionValues := buildCondition(c.whereConditions)
	buf.WriteString(where)
	buf.WriteString(condition)

	if c.limitNumber > 0 {
		buf.WriteString(limit)
		buf.WriteString(strconv.Itoa(c.limitNumber))
	}

	if c.allowFiltering {
		buf.WriteString(allowFiltering)
	}

	return buf.String(), conditionValues, nil
}
