package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Tuingking/qbuilder"
	_ "github.com/go-sql-driver/mysql"
)

const (
	username = "root"
	password = "password"
	host     = "127.0.0.1"
	port     = "3306"
	database = "playground"
)

func main() {
	connstr := "{username}:{password}@tcp({host}:{port})/{database}"

	replacer := strings.NewReplacer(
		"{username}", username,
		"{password}", password,
		"{host}", host,
		"{port}", port,
		"{database}", database,
	)

	dataSourceName := replacer.Replace(connstr)

	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err)
	}

	if err := db.Ping(); err != nil {
		panic(err)
	}
	defer db.Close()

	query := `SELECT id, name FROM product`

	param := ProductParam{
		ID: sql.NullInt64{Int64: 1, Valid: true},
	}

	qb := qbuilder.New()
	wc, args, err := qb.Build(&param)
	if err != nil {
		fmt.Println("failed query builder", err)
		return
	}

	rows, err := db.QueryContext(context.Background(), query+wc, args...)
	if err != nil {
		fmt.Println("failed query", err)
		return
	}

	var products []Product
	for rows.Next() {
		var p Product
		if err = rows.Scan(&p.ID, &p.Name); err != nil {
			fmt.Println("failed scan", err)
		}
		products = append(products, p)
	}

	fmt.Printf("products: %+v\n", products)
}

type Product struct {
	ID   int64
	Name string
}

type ProductParam struct {
	ID   sql.NullInt64  `param:"id" db:"id"`
	Name sql.NullString `param:"name" db:"name"`
}
