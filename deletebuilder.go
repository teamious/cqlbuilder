package cqlbuilder

import (
	"bytes"
)

//The type of delete builder which wrap the delete CQL statement.
// TODO :  support timestamp.
type DeleteBuilder struct {
	colums          []string
	table           string
	ifConditions    []conditionBuilder
	whereConditions []conditionBuilder
}

// The add delete column.
func (c *DeleteBuilder) DeleteColumn(columnName string) *DeleteBuilder {
	c.colums = append(c.colums, columnName)
	return c
}

// Add where clause
func (c *DeleteBuilder) Where(con conditionBuilder) *DeleteBuilder {
	c.whereConditions = append(c.whereConditions, con)
	return c
}

// Add if clause
func (c *DeleteBuilder) If(con conditionBuilder) *DeleteBuilder {
	c.ifConditions = append(c.ifConditions, con)
	return c
}

// Validate
func (c *DeleteBuilder) Validate() error {
	if len(c.table) == 0 {
		return errEmptyTable
	}

	if len(c.whereConditions) == 0 {
		return errEmptyCondition
	}
	return nil
}

// Build the query string and construct the value lists.
// sth like :
// DELETE firstname, lastname FROM cycling.cyclist_name WHERE firstname = 'Alex'
// IF EXISTS AND version=123;
func (c *DeleteBuilder) ToQuery() (string, []interface{}, error) {

	if err := c.Validate(); err != nil {
		return "", nil, err
	}

	var buf bytes.Buffer
	values := make([]interface{}, 0, len(c.ifConditions)+len(c.whereConditions))

	buf.WriteString(deleteKW)

	for i, col := range c.colums {
		if i > 0 {
			buf.WriteString(comma)
		}
		buf.WriteString(col)
	}

	buf.WriteString(from)
	buf.WriteString(c.table)

	if len(c.whereConditions) > 0 {
		buf.WriteString(where)

		condition, conditionValues := buildCondition(c.whereConditions)
		buf.WriteString(condition)
		values = append(values, conditionValues...)
	}

	if len(c.ifConditions) > 0 {
		buf.WriteString(ifs)

		condition, conditionValues := buildCondition(c.ifConditions)
		buf.WriteString(condition)
		values = append(values, conditionValues...)
	}

	return buf.String(), values, nil
}
