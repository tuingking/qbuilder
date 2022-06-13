package qbuilder

import (
	"errors"
	"fmt"
	"reflect"
)

const (
	defaultPage  int64 = 1
	defaultLimit int64 = 10

	whereClauseFmt      = " AND %s %s ?"
	whereClauseMultiFmt = " AND %s %s (?)"
)

type queryBuilder struct {
	page   int64
	limit  int64
	sortBy []string

	// custom where clause
	customWhereClause     []string
	customWhereClauseArgs []interface{}

	// option
	extraLimit int64

	// result
	args        []interface{}
	whereClause string
}

func New(opts ...Option) *queryBuilder {
	qb := &queryBuilder{
		whereClause: " WHERE 1=1",
		page:        defaultPage,
		limit:       defaultLimit,
	}

	for _, opt := range opts {
		opt(qb)
	}

	return qb
}

type Option func(*queryBuilder)

// WithExtraLimit will add 1 extra row.
// The purpose is for checking whether in the next page still has data or not.
//
// e.g: if page=1 and limit=10, it will return LIMIT 0, 11
func WithExtraLimit() Option {
	return func(qb *queryBuilder) {
		qb.extraLimit = 1
	}
}

// Add custom where clause
func (q *queryBuilder) AddWhereClause(wc string, args ...interface{}) *queryBuilder {
	q.customWhereClause = append(q.customWhereClause, wc)
	q.customWhereClauseArgs = append(q.customWhereClauseArgs, args...)

	return q
}

func (q *queryBuilder) handleParamPage(field reflect.Value) int64 {
	page := defaultPage

	switch val := field.Interface().(type) {
	case int64:
		if val > 0 {
			page = val
		}
	case int:
		if val > 0 {
			page = int64(val)
		}
	default:
		// use default page
	}
	return page
}

func (q *queryBuilder) handleParamLimit(field reflect.Value) int64 {
	limit := defaultLimit

	switch val := field.Interface().(type) {
	case int64:
		if val > 0 {
			limit = val
		}
	case int:
		if val > 0 {
			limit = int64(val)
		}
	default:
		// use default limit
	}
	return limit
}

func (q *queryBuilder) handleParamShortBy(field reflect.Value) []string {
	var shortBy []string

	if val, ok := field.Interface().([]string); ok {
		shortBy = val
	} else {
		// default []string
	}

	return shortBy
}

func (q *queryBuilder) makeOrderByClause() string {
	var orderByClause string

	if len(q.sortBy) > 0 {
		orderByClause += " ORDER BY "
		for i, v := range q.sortBy {
			if i > 0 {
				orderByClause += ", "
			}

			if v[0] == '-' {
				orderByClause += v[1:] + " DESC"
			} else {
				orderByClause += v + " ASC"
			}
		}
	}

	return orderByClause
}

func (q *queryBuilder) makeLimitClause() string {
	offset := (q.page - 1) * q.limit
	limitClause := fmt.Sprintf(" LIMIT %d, %d", offset, offset+q.limit+q.extraLimit)

	return limitClause
}

func (q *queryBuilder) appendCustomWhere() {
	for _, wc := range q.customWhereClause {
		q.whereClause += " AND " + wc
	}
	q.args = append(q.args, q.customWhereClauseArgs...)
}

func (q *queryBuilder) Build(param interface{}) (sqlClause string, args []interface{}, err error) {
	p := reflect.ValueOf(param)
	if p.Kind() != reflect.Ptr || p.IsNil() {
		return sqlClause, args, errors.New("should be a pointer and cannot be nil")
	}

	val := reflect.ValueOf(param).Elem()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		structTags := val.Type().Field(i).Tag // param:"created_at__gte" db:"created_at"
		tagParam := structTags.Get("param")   // created_at__lte
		tagDB := structTags.Get("db")         // created_at

		c := newCursor(field, tagParam, tagDB)

		if c.IsPage() {
			q.page = q.handleParamPage(field)
			continue
		}

		if c.IsLimit() {
			q.limit = q.handleParamLimit(field)
			continue
		}

		if c.IsSortBy() {
			q.sortBy = q.handleParamShortBy(field)
			continue
		}

		if c.IsEmpty() {
			continue
		}

		clause, args, skip := c.Make()
		if skip {
			continue
		}
		q.whereClause += clause
		q.args = append(q.args, args...)
	}

	// custom where
	q.appendCustomWhere()

	// result
	sqlClause = q.whereClause + q.makeOrderByClause() + q.makeLimitClause()

	fmt.Println("[qbuilder] clause: ", sqlClause)
	fmt.Println("[qbuilder] args: ", q.args)

	return sqlClause, q.args, nil
}

func ValidatePageAndLimit(p, l int64) (page int64, limit int64) {
	page, limit = p, l
	if page == 0 {
		page = 1
	}
	if limit == 0 {
		limit = 10
	}
	return page, limit
}
