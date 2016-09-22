# cqlbuilder v0.0.1

A light wrap over gocql library to help build/exec cql. 

currently, it include:
1. query/iter/
2. exec upsert
3. Has Execution manager for UT mock.

# Here is a sample for iter

```go
import (
...
	cb "cqlbuilder"

)

// The server class handle the Wx CRUD request.
type server struct {
	execMgr cb.ExecManager
}
// retrieve workxtreams for certain org.
func (c *server) GetXXX(ctx context.Context, ...) (..., error) {
...
	sel := cb.Select(orgWorkxtreamTbl).AddColumn(wxIDCol).AddColumn(rootBodyCol).AddColumn(versionCol).AddColumn(updateTimeCol).Where(cb.Eq(orgIdCol, orgid))

	iter, err := c.execMgr.Iter(sel)

	if err != nil {
		log.CxtErrorln(ctx, "Failed exec query to retrieve Workxtream list", err)
		resp.Error = errcode.ErrCqlFailure
		return resp, nil
	}
	for iter.Scan(&wxt.Id, &body, &wxt.WxVersion, &wxt.UpdateTime) {
...
	}

...
```

Please let us know if you find any bug or has any feature request.

