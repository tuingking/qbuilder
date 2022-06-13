# qbuilder

query builder for MySQL

## Features

* generate where clause and the arguments for `SELECT` query based on the params

## Examples

```Go

type FooParam struct {
    Name sql.NullString `param:"name" db:"name"`
    Age  sql.NullInt64  `param:"age" db:"age"`
}

func GetFoo() {

    // ...

    p := FooParam{
        Name: sql.NullString{Valid:true, String: "foo"}
        Age: sql.NullInt64{Valid:true, Int64: 17}
    }
    
    qb := qbuilder.New()
    wc, args, err := qb.Build(&param)
    if err != nil {
        panic(err)
    }

    fmt.Println("wc", wc)   // WHERE 1=1 AND name=? AND age=? LIMIT 0,10
    fmt.Println("args", wc) // ["foo", 17]
    
    // ...
}

```