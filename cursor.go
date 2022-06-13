package qbuilder

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

type cursor struct {
	field reflect.Value // struct field
	param string        // tag:"param"
	db    string        // tag:"db"
}

func newCursor(field reflect.Value, param, db string) cursor {
	return cursor{
		field: field,
		param: param,
		db:    db,
	}
}

func (c *cursor) IsPage() bool {
	return c.param == "page"
}

func (c *cursor) IsLimit() bool {
	return c.param == "limit"
}

func (c *cursor) IsSortBy() bool {
	return c.param == "short_by"
}

func (c *cursor) IsEmpty() bool {
	if c.param == "-" ||
		c.param == "" ||
		c.db == "" ||
		c.db == "-" {
		return true
	}
	return false
}

func (c *cursor) GetOperand() string {
	operand := "="

	// update operand
	switch param := c.param; {
	case strings.HasSuffix(param, "__gt"):
		operand = ">"
	case strings.HasSuffix(param, "__gte"):
		operand = ">="
	case strings.HasSuffix(param, "__lt"):
		operand = "<"
	case strings.HasSuffix(param, "__lte"):
		operand = "<="
	case strings.HasSuffix(param, "__neq"):
		operand = "!="
	default:
	}

	return operand
}

func (c *cursor) GetOperandMulti() string {
	operand := "IN"

	// update operand
	if strings.HasSuffix(c.param, "__nin") {
		operand = "NOT IN"
	} else {
		// skip
	}

	return operand
}

func (c *cursor) Make() (clause string, args []interface{}, skip bool) {
	skip = true

	switch c.field.Interface().(type) {
	case string, int, int32, int64, float32, float64:
		clause, args, skip = c.makeClausePrimitiveType()
	case time.Time, sql.NullTime:
		clause, args, skip = c.makeClauseTimeType()
	case []string, []int, []int32, []int64, []float32, []float64:
		clause, args, skip = c.makeClauseArrayType()
	case sql.NullString, sql.NullInt32, sql.NullInt64, sql.NullFloat64:
		clause, args, skip = c.makeClauseSqlNullType()
	default:
	}

	return
}

func (c *cursor) makeClausePrimitiveType() (clause string, args []interface{}, skip bool) {
	skip = true
	operand := c.GetOperand()

	switch val := c.field.Interface().(type) {
	case string:
		clause, args, skip = c.makeClauseString(whereClauseFmt, operand, val)
	case int, int32, int64, float32, float64:
		clause, args, skip = c.makeClause(whereClauseFmt, operand, val)
	default:
	}

	return
}

func (c *cursor) makeClauseTimeType() (clause string, args []interface{}, skip bool) {
	skip = true
	operand := c.GetOperand()

	switch val := c.field.Interface().(type) {
	case time.Time:
		clause, args, skip = c.makeClauseTime(whereClauseFmt, operand, val)
	case sql.NullTime:
		clause, args, skip = c.makeClauseNullTime(whereClauseFmt, operand, val)
	default:
	}

	return
}

func (c *cursor) makeClauseArrayType() (clause string, args []interface{}, skip bool) {
	skip = true

	switch val := c.field.Interface().(type) {
	case []string, []int, []int32, []int64, []float32, []float64:
		clause, args, skip = c.makeClauseMulti(val)
	default:
	}

	return
}

func (c *cursor) makeClauseSqlNullType() (clause string, args []interface{}, skip bool) {
	skip = true
	operand := c.GetOperand()

	switch val := c.field.Interface().(type) {
	case sql.NullString:
		clause, args, skip = c.makeClauseNullString(whereClauseFmt, operand, val)
	case sql.NullInt32:
		clause, args, skip = c.makeClauseNullInt32(whereClauseFmt, operand, val)
	case sql.NullInt64:
		clause, args, skip = c.makeClauseNullInt64(whereClauseFmt, operand, val)
	case sql.NullFloat64:
		clause, args, skip = c.makeClauseNullFloat64(whereClauseFmt, operand, val)
	default:
	}

	return
}

func (c *cursor) makeClause(layout, operand string, val interface{}) (clause string, args []interface{}, skip bool) {
	clause = fmt.Sprintf(layout, c.db, operand)
	args = append(args, val)

	return
}

func (c *cursor) makeClauseString(layout, operand string, val string) (clause string, args []interface{}, skip bool) {
	if c.field.String() == "" {
		skip = true
		return
	}

	if operand == "=" {
		operand = "LIKE"
	}

	return c.makeClause(layout, operand, val)
}

func (c *cursor) makeClauseTime(layout, operand string, val time.Time) (clause string, args []interface{}, skip bool) {
	if val.IsZero() {
		skip = true
		return
	}

	return c.makeClause(layout, operand, val)
}

func (c *cursor) makeClauseNullTime(layout, operand string, val sql.NullTime) (clause string, args []interface{}, skip bool) {
	if !val.Valid {
		skip = true
		return
	}

	return c.makeClause(layout, operand, val.Time)
}

func (c *cursor) makeClauseMulti(val interface{}) (clause string, args []interface{}, skip bool) {
	operandMulti := c.GetOperandMulti()
	tempQuery := fmt.Sprintf(whereClauseMultiFmt, c.db, operandMulti)
	tempQuery, tempArgs, _ := sqlx.In(tempQuery, val)
	clause = tempQuery
	if len(tempArgs) < 1 {
		skip = true
	}
	args = append(args, tempArgs...)

	return
}

func (c *cursor) makeClauseNullString(layout, operand string, val sql.NullString) (clause string, args []interface{}, skip bool) {
	if !val.Valid {
		skip = true
		return
	}
	if operand == "=" {
		operand = "LIKE"
	}

	return c.makeClause(layout, operand, val.String)
}

func (c *cursor) makeClauseNullInt32(layout, operand string, val sql.NullInt32) (clause string, args []interface{}, skip bool) {
	if !val.Valid {
		skip = true
		return
	}

	return c.makeClause(layout, operand, val.Int32)
}

func (c *cursor) makeClauseNullInt64(layout, operand string, val sql.NullInt64) (clause string, args []interface{}, skip bool) {
	if !val.Valid {
		skip = true
		return
	}

	return c.makeClause(layout, operand, val.Int64)
}

func (c *cursor) makeClauseNullFloat64(layout, operand string, val sql.NullFloat64) (clause string, args []interface{}, skip bool) {
	if !val.Valid {
		skip = true
		return
	}

	return c.makeClause(layout, operand, val.Float64)
}
