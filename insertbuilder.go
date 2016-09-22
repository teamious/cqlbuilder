package cqlbuilder

import (
	"bytes"
)

//The type of insert builder which wrap the insert CQL statement.
type InsertBuilder struct {
	colums      []string
	values      []interface{}
	table       string
	ifNotExists bool
	ttl         int
}

// Set a value
//  If the value has already been set, new value will overwrite old one.
func (c *InsertBuilder) SetValue(columnName string, value interface{}) *InsertBuilder {
	c.values = append(c.values, value)
	c.colums = append(c.colums, columnName)
	return c
}

func (c *InsertBuilder) SetTtl(t int) *InsertBuilder {
	c.ttl = t
	return c
}

// Set if not exists
func (c *InsertBuilder) IfNotExists(e bool) *InsertBuilder {
	c.ifNotExists = e
	return c
}

// Validate
func (c *InsertBuilder) Validate() error {
	if len(c.table) == 0 {
		return errEmptyTable
	}

	if len(c.colums) == 0 {
		return errEmptyColumn
	}

	return nil
}

// Build the query string and construct the value lists.
func (c *InsertBuilder) ToQuery() (string, []interface{}, error) {
	if err := c.Validate(); err != nil {
		return "", nil, err
	}

	var buf, vals bytes.Buffer
	values := make([]interface{}, 0, len(c.values))

	buf.WriteString(insert)
	buf.WriteString(c.table)
	buf.WriteString(leftPar)
	vals.WriteString(leftPar)

	for i := 0; i < len(c.colums); i++ {
		if i > 0 {
			buf.WriteString(comma)
			vals.WriteString(comma)
		}
		buf.WriteString(c.colums[i])
		vals.WriteString(questMak)
	}

	values = append(values, c.values...)

	buf.WriteString(rightPar)
	vals.WriteString(rightPar)

	buf.WriteString(valuesSt)
	buf.Write(vals.Bytes())

	if c.ifNotExists == true {
		buf.WriteString(ifNotExists)
	}

	if c.ttl > 0 {
		buf.WriteString(using)
		buf.WriteString(ttl)
		values = append(values, c.ttl)
	}

	return buf.String(), values, nil
}
