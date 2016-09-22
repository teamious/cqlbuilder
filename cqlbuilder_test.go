package cqlbuilder

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	cql "github.com/gocql/gocql"
)

func TestInsert(t *testing.T) {
	ins := Insert("test")
	ins.SetValue("col1", "test").SetValue("col2", 4123)

	str, vals, err := ins.ToQuery()

	if err != nil {
		t.Logf("err: %s", err)
		t.FailNow()
	}

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("INSERT INTO test(col1,col2) VALUES(?,?)") || len(vals) != 2 {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestInsertWithIf(t *testing.T) {
	ins := Insert("test")
	ins.SetValue("col1", "test").SetValue("col2", 4123).IfNotExists(true)

	str, vals, err := ins.ToQuery()

	if err != nil {
		t.Logf("err: %s", err)
		t.FailNow()
	}

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("INSERT INTO test(col1,col2) VALUES(?,?) IF NOT EXISTS") || len(vals) != 2 {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestDeleteWithoutIf(t *testing.T) {

	del := Delete("test")
	del.DeleteColumn("col1").DeleteColumn("col2").Where(Eq("col3", "value3")).Where(Eq("col4", 1))

	str, vals, _ := del.ToQuery()

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("DELETE col1,col2 FROM test WHERE col3=? AND col4=?") || len(vals) != 2 || vals[0] != "value3" || vals[1] != 1 {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestDeleteWithIf(t *testing.T) {

	del := Delete("test")
	del.DeleteColumn("col1").DeleteColumn("col2").Where(Eq("col3", "value3")).Where(Eq("col4", 1)).If(Exists()).If(Eq("Version", 123))

	str, vals, _ := del.ToQuery()

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("DELETE col1,col2 FROM test WHERE col3=? AND col4=? IF  EXISTS  AND Version=?") || len(vals) != 3 || vals[0] != "value3" || vals[1] != 1 || vals[2] != 123 {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestInsertWithTTL(t *testing.T) {
	ins := Insert("test")
	ins.SetValue("col1", "test").SetValue("col2", 4123).IfNotExists(true).SetTtl(100)

	str, vals, err := ins.ToQuery()

	if err != nil {
		t.Logf("err: %s", err)
		t.FailNow()
	}

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("INSERT INTO test(col1,col2) VALUES(?,?) IF NOT EXISTS  USING  TTL ?") || len(vals) != 3 {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestUpdate(t *testing.T) {
	up := Update("test")
	str, vals, err := up.SetValue("col1", 123).SetValue("col2", "test").Where(Eq("col3", 456)).Where(Eq("col4", "thisthevalue")).ToQuery()

	if err != nil {
		t.Logf("err: %s", err)
		t.FailNow()
	}

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("UPDATE test SET col1 =? ,col2 =?  WHERE col3=? and col4=?") || len(vals) != 4 {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestUpdateWithWhereAndIfExists(t *testing.T) {
	up := Update("test")
	str, vals, err := up.SetValue("col1", 123).SetValue("col2", "test").Where(Eq("col3", 456)).Where(Eq("col4", "thisthevalue")).If(Exists()).ToQuery()

	if err != nil {
		t.Logf("err: %s", err)
		t.FailNow()
	}

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("UPDATE test SET col1 =? ,col2 =?  WHERE col3=? AND col4=? IF  EXISTS") || len(vals) != 4 {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestUpdateWithWhereAndIfCond(t *testing.T) {
	up := Update("test")
	str, vals, err := up.SetValue("col1", 123).SetValue("col2", "test").Where(Eq("col3", 456)).Where(Eq("col4", "thisthevalue")).If(Eq("Version", 123)).ToQuery()

	if err != nil {
		t.Logf("err: %s", err)
		t.FailNow()
	}

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("UPDATE test SET col1 =? ,col2 =?  WHERE col3=? AND col4=? IF Version=?") || len(vals) != 5 {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestUpdateWithoutWhere(t *testing.T) {
	up := Update("test")
	_, _, err := up.SetValue("col1", 123).SetValue("col2", "test").ToQuery()
	if err == nil {
		t.Logf("Err expected if Update without where")
		t.FailNow()
	}
}

func TestSelect(t *testing.T) {
	se := Select("test")
	str, vals, err := se.AddColumn("col1").AddColumn("col2").AddColumn("Col3").Where(Eq("Col4", 4)).Where(Eq("Col5", "test")).ToQuery()

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("SELECT col1,col2,Col3 FROM test WHERE Col4=? AND Col5=?") || vals[0] != 4 || vals[1] != "test" || err != nil {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestSelectWithLimit(t *testing.T) {
	se := Select("test")
	str, vals, err := se.AddColumn("col1").AddColumn("col2").AddColumn("Col3").Where(Eq("Col4", 4)).Where(Eq("Col5", "test")).SetLimit(100).ToQuery()

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("SELECT col1,col2,Col3 FROM test WHERE Col4=? AND Col5=? LIMIT 100") || vals[0] != 4 || vals[1] != "test" || err != nil {
		t.Logf("str %s  vals %V", str, vals)
		t.FailNow()
	}
}

func TestSelectWithIn(t *testing.T) {
	se := Select("Test")
	str, vals, err := se.AddColumn("Col1").Where(In("Col2", []int{123, 456})).Where(Eq("Col3", 123)).ToQuery()

	if strings.Trim(strings.ToLower(str), " ") != strings.ToLower("SELECT Col1 FROM Test WHERE Col2 in ? AND Col3=?") || vals[1] != 123 || err != nil {
		t.Logf("failed %v %v %v ", str, vals, err)
		t.FailNow()
	}
}

// This is a sample code to use the builder/exec.  Not expect run as part of UT. Need actually connect to Cassandra and prepare the keyspace/table.
// To run this, either change the method name or invoke it from a test case.
// keyspace :  CREATE KEYSPACE k   WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// table:      create table k.t1 ( c1 text primary key, c2 text, c3 text, todo map<timestamp, text>)
func SampleCode(t *testing.T) {
	cluster := cql.NewCluster("127.0.0.1")
	cluster.Keyspace = "k"
	session, err := cluster.CreateSession()

	if err != nil {
		fmt.Println(err)
		return
	}

	//Sample of batch.
	batch := StartBatch()
	for i := 0; i < 10; i++ {
		ins := Insert("k.t1")
		ins.SetValue("c1", cql.TimeUUID().String())
		ins.SetValue("c2", strconv.Itoa(i))
		batch.Add(ins)
	}
	err = ExecBatch(batch, session)
	if err != nil {
		fmt.Println(err)
	}

	//Sample of insert
	key := "abc" + cql.TimeUUID().String()
	ins2 := Insert("k.t1")
	ins2.SetValue("c1", key).SetValue("c2", "C2")
	err = Exec(ins2, session)
	if err != nil {
		fmt.Println(err)
	}

	//Sample of update
	up := Update("k.t1")
	up.SetValue("c3", "ccccTTTT").Where(Eq("c1", key))
	err = Exec(up, session)
	if err != nil {
		fmt.Println(err)
	}

	//Sample of insert
	key = "def" + cql.TimeUUID().String()
	ins2 = Insert("k.t1")
	ins2.SetValue("c1", key).SetValue("c2", "def").IfNotExists(true)
	err = Exec(ins2, session)
	if err != nil {
		fmt.Println(err)
	}

	ins2 = Insert("k.t1")
	ins2.SetValue("c1", key).SetValue("c2", "def other value").IfNotExists(true)
	err = Exec(ins2, session)
	if err != nil {
		fmt.Println(err)
	}

	ins2 = Insert("k.t1")
	ins2.SetValue("c1", key).SetValue("c2", "def other value").IfNotExists(false).SetTtl(10)
	err = Exec(ins2, session)
	if err != nil {
		fmt.Println(err)
	}
}
