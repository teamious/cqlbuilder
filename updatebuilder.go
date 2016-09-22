package cqlbuilder

import (
	"bytes"
	"errors"
)

// The update builder.
type UpdateBuilder struct {
	colums          []string
	values          []interface{}
	whereConditions []conditionBuilder
	ifConditions    []conditionBuilder
	table           string
}

// Set a value
// If the value already been set, new value will overwrite old one.
func (c *UpdateBuilder) SetValue(name string, value interface{}) *UpdateBuilder {
	c.colums = append(c.colums, name)
	c.values = append(c.values, value)
	return c
}

// Set the where condition.
func (c *UpdateBuilder) Where(condition conditionBuilder) *UpdateBuilder {
	c.whereConditions = append(c.whereConditions, condition)

	return c
}

// Add if clause
func (c *UpdateBuilder) If(condition conditionBuilder) *UpdateBuilder {
	c.ifConditions = append(c.ifConditions, condition)
	return c
}

// Validate
func (c *UpdateBuilder) Validate() error {
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

// Build the update query statement string and values.
// Example:
//
func (c *UpdateBuilder) ToQuery() (string, []interface{}, error) {
	if err := c.Validate(); err != nil {
		return "", nil, err
	}

	var buf bytes.Buffer
	values := make([]interface{}, 0, len(c.values))

	buf.WriteString(update)
	buf.WriteString(c.table)
	buf.WriteString(set)

	for i := 0; i < len(c.colums); i++ {
		if len(c.colums[i]) == 0 {
			return "", nil, errors.New("Column name can't be nil")
		}

		if i > 0 {
			buf.WriteString(comma)
		}

		buf.WriteString(c.colums[i])
		buf.WriteString(eq)
	}

	values = append(values, c.values...)

	condition, conditionValues := buildCondition(c.whereConditions)
	buf.WriteString(where)
	buf.WriteString(condition)
	values = append(values, conditionValues...)

	if len(c.ifConditions) > 0 {
		condition, conditionValues = buildCondition(c.ifConditions)
		buf.WriteString(ifs)
		buf.WriteString(condition)
		values = append(values, conditionValues...)
	}

	return buf.String(), values, nil
}
