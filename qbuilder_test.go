package qbuilder

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type ParamSkip struct {
	String string `param:"string" db:"-"`
}

type ParamPrimitive struct {
	String  string  `param:"string" db:"string"`
	Int     int     `param:"int" db:"int"`
	Int32   int32   `param:"int32" db:"int32"`
	Int64   int64   `param:"int64" db:"int64"`
	Float32 float32 `param:"float32" db:"float32"`
	Float64 float64 `param:"float64" db:"float64"`
}

type ParamTime struct {
	Time     time.Time    `param:"time" db:"time"`
	NullTime sql.NullTime `param:"nulltime" db:"nulltime"`
}

type ParamArr struct {
	Strings  []string  `param:"strings" db:"strings"`
	Ints     []int     `param:"ints" db:"ints"`
	Int32s   []int32   `param:"int32s" db:"int32s"`
	Int64s   []int64   `param:"int64s" db:"int64s"`
	Float32s []float32 `param:"float32s" db:"float32s"`
	Float64s []float64 `param:"float64s" db:"float64s"`
}

type ParamNull struct {
	NullString  sql.NullString  `param:"nullstring" db:"nullstring"`
	NullInt32   sql.NullInt32   `param:"nullint32" db:"nullint32"`
	NullInt64   sql.NullInt64   `param:"nullint64" db:"nullint64"`
	NullFloat64 sql.NullFloat64 `param:"nullfloat64" db:"nullfloat64"`
}

type ParamPaginationInt64 struct {
	Page    int64    `param:"page"`
	Limit   int64    `param:"limit"`
	ShortBy []string `param:"short_by"`
}

type ParamPaginationInt struct {
	Page    int      `param:"page"`
	Limit   int      `param:"limit"`
	ShortBy []string `param:"short_by"`
}

type ParamOperand struct {
	Int64GTE  sql.NullInt64  `param:"int64__gte" db:"int64"`
	Int64LTE  sql.NullInt64  `param:"int64__lte" db:"int64"`
	Int64GT   sql.NullInt64  `param:"int64__gt" db:"int64"`
	Int64LT   sql.NullInt64  `param:"int64__lt" db:"int64"`
	StringNIN []string       `param:"string__nin" db:"string"`
	StringNEQ sql.NullString `param:"string__neq" db:"string"`
}

func Test_QBuilder_SkipField(t *testing.T) {
	param := ParamSkip{
		String: "test",
	}
	expClause := " WHERE 1=1 LIMIT 0, 10"

	clause, args, err := New().Build(&param)
	assert.Nil(t, err)
	assert.Equal(t, expClause, clause)
	assert.Nil(t, args)
}

func Test_QBuilder(t *testing.T) {
	t.Run("Pointer Param", func(t *testing.T) {
		p := ParamPrimitive{}
		_, _, err := New().Build(&p)
		assert.Nil(t, err)
	})

	t.Run("NOT Pointer Param", func(t *testing.T) {
		p := ParamPrimitive{}
		_, _, err := New().Build(p)
		assert.NotNil(t, err)
	})
}

func Test_QBuilder_Primitive(t *testing.T) {
	param := ParamPrimitive{
		String:  "test",
		Int:     10,
		Int32:   20,
		Int64:   30,
		Float32: 40.22,
		Float64: 50.22,
	}
	expClause := " WHERE 1=1 AND string LIKE ? AND int = ? AND int32 = ? AND int64 = ? AND float32 = ? AND float64 = ? LIMIT 0, 10"
	expArgs := []interface{}{"test", 10, int32(20), int64(30), float32(40.22), float64(50.22)}

	clause, args, err := New().Build(&param)
	assert.Nil(t, err)
	assert.Equal(t, expClause, clause)
	assert.Equal(t, expArgs, args)
}

func Test_QBuilder_Array(t *testing.T) {
	testCase := []struct {
		desc      string
		param     ParamArr
		expClause string
		expArgs   []interface{}
	}{
		{
			desc: "success",
			param: ParamArr{
				Strings:  []string{"test-1", "test-2"},
				Ints:     []int{10, 11},
				Int32s:   []int32{20, 21},
				Int64s:   []int64{30, 31},
				Float32s: []float32{40.22, 41.22},
				Float64s: []float64{50.22, 51.22},
			},
			expClause: " WHERE 1=1 AND strings IN (?, ?) AND ints IN (?, ?) AND int32s IN (?, ?) AND int64s IN (?, ?) AND float32s IN (?, ?) AND float64s IN (?, ?) LIMIT 0, 10",
			expArgs:   []interface{}{"test-1", "test-2", 10, 11, int32(20), int32(21), int64(30), int64(31), float32(40.22), float32(41.22), float64(50.22), float64(51.22)},
		},
		{
			desc: "empty array",
			param: ParamArr{
				Strings:  []string{},
				Ints:     []int{},
				Int32s:   []int32{},
				Int64s:   []int64{},
				Float32s: []float32{},
				Float64s: []float64{},
			},
			expClause: " WHERE 1=1 LIMIT 0, 10",
			expArgs:   nil,
		},
	}

	for i, tc := range testCase {
		t.Run(fmt.Sprintf("[%d] %s", i, tc.desc), func(t *testing.T) {
			clause, args, err := New().Build(&tc.param)
			assert.Nil(t, err)
			assert.Equal(t, tc.expClause, clause)
			assert.Equal(t, tc.expArgs, args)
		})
	}
}

func Test_QBuilder_SqlNull(t *testing.T) {
	param := ParamNull{
		NullString:  sql.NullString{Valid: true, String: "test"},
		NullInt32:   sql.NullInt32{Valid: true, Int32: 20},
		NullInt64:   sql.NullInt64{Valid: true, Int64: 30},
		NullFloat64: sql.NullFloat64{Valid: true, Float64: 50.22},
	}
	expClause := " WHERE 1=1 AND nullstring LIKE ? AND nullint32 = ? AND nullint64 = ? AND nullfloat64 = ? LIMIT 0, 10"
	expArgs := []interface{}{"test", int32(20), int64(30), float64(50.22)}

	clause, args, err := New().Build(&param)
	assert.Nil(t, err)
	assert.Equal(t, expClause, clause)
	assert.Equal(t, expArgs, args)
}

func Test_QBuilder_Time(t *testing.T) {
	testCase := []struct {
		desc      string
		param     ParamTime
		expClause string
		expArgs   []interface{}
	}{
		{
			desc: "success",
			param: ParamTime{
				Time:     time.Date(2022, 06, 19, 10, 0, 0, 0, time.Local),
				NullTime: sql.NullTime{Valid: true, Time: time.Date(2022, 06, 19, 10, 0, 0, 0, time.Local)},
			},
			expClause: " WHERE 1=1 AND time = ? AND nulltime = ? LIMIT 0, 10",
			expArgs:   []interface{}{time.Date(2022, time.June, 19, 10, 0, 0, 0, time.Local), time.Date(2022, time.June, 19, 10, 0, 0, 0, time.Local)},
		},
		{
			desc: "zero time & invalid time",
			param: ParamTime{
				Time: time.Time{},
			},
			expClause: " WHERE 1=1 LIMIT 0, 10",
			expArgs:   nil,
		},
	}

	for i, tc := range testCase {
		t.Run(fmt.Sprintf("[%d] %s", i, tc.desc), func(t *testing.T) {
			clause, args, err := New().Build(&tc.param)
			assert.Nil(t, err)
			assert.Equal(t, tc.expClause, clause)
			assert.Equal(t, tc.expArgs, args)
		})
	}
}

func Test_QBuilder_PaginationInt64(t *testing.T) {
	testCase := []struct {
		desc      string
		param     ParamPaginationInt64
		expClause string
	}{
		{
			desc: "first page",
			param: ParamPaginationInt64{
				Page:  1,
				Limit: 100,
			},
			expClause: " WHERE 1=1 LIMIT 0, 100",
		},
		{
			desc: "second page",
			param: ParamPaginationInt64{
				Page:  2,
				Limit: 100,
			},
			expClause: " WHERE 1=1 LIMIT 100, 200",
		},
		{
			desc: "order by asc",
			param: ParamPaginationInt64{
				ShortBy: []string{"status"},
			},
			expClause: " WHERE 1=1 ORDER BY status ASC LIMIT 0, 10",
		},
		{
			desc: "order by desc",
			param: ParamPaginationInt64{
				ShortBy: []string{"-created_at"},
			},
			expClause: " WHERE 1=1 ORDER BY created_at DESC LIMIT 0, 10",
		},
		{
			desc: "multiple order by",
			param: ParamPaginationInt64{
				ShortBy: []string{"-created_at", "status"},
			},
			expClause: " WHERE 1=1 ORDER BY created_at DESC, status ASC LIMIT 0, 10",
		},
	}

	for _, tc := range testCase {
		clause, args, err := New().Build(&tc.param)
		assert.Nil(t, err)
		assert.Equal(t, tc.expClause, clause)
		assert.Nil(t, args)
	}
}

func Test_QBuilder_PaginationInt(t *testing.T) {
	testCase := []struct {
		desc      string
		param     ParamPaginationInt
		expClause string
	}{
		{
			desc: "first page",
			param: ParamPaginationInt{
				Page:  1,
				Limit: 100,
			},
			expClause: " WHERE 1=1 LIMIT 0, 100",
		},
	}

	for _, tc := range testCase {
		clause, args, err := New().Build(&tc.param)
		assert.Nil(t, err)
		assert.Equal(t, tc.expClause, clause)
		assert.Nil(t, args)
	}
}

func Test_QBuilder_Operand(t *testing.T) {
	testCase := []struct {
		desc      string
		param     ParamOperand
		expClause string
		expArgs   []interface{}
	}{
		{
			desc: "> AND <",
			param: ParamOperand{
				Int64GT: sql.NullInt64{Valid: true, Int64: 10},
				Int64LT: sql.NullInt64{Valid: true, Int64: 20},
			},
			expClause: " WHERE 1=1 AND int64 > ? AND int64 < ? LIMIT 0, 10",
			expArgs:   []interface{}{int64(10), int64(20)},
		},
		{
			desc: ">= AND <=",
			param: ParamOperand{
				Int64GTE: sql.NullInt64{Valid: true, Int64: 10},
				Int64LTE: sql.NullInt64{Valid: true, Int64: 20},
			},
			expClause: " WHERE 1=1 AND int64 >= ? AND int64 <= ? LIMIT 0, 10",
			expArgs:   []interface{}{int64(10), int64(20)},
		},
		{
			desc: "NOT IN",
			param: ParamOperand{
				StringNIN: []string{"ACTIVE", "USED"},
			},
			expClause: " WHERE 1=1 AND string NOT IN (?, ?) LIMIT 0, 10",
			expArgs:   []interface{}{"ACTIVE", "USED"},
		},
		{
			desc: "!=",
			param: ParamOperand{
				StringNEQ: sql.NullString{Valid: true, String: "HOHO"},
			},
			expClause: " WHERE 1=1 AND string != ? LIMIT 0, 10",
			expArgs:   []interface{}{"HOHO"},
		},
	}

	for _, tc := range testCase {
		clause, args, err := New().Build(&tc.param)
		assert.Nil(t, err)
		assert.Equal(t, tc.expClause, clause)
		assert.Equal(t, tc.expArgs, args)
	}
}

func Test_QBuilder_WithExtraLimit(t *testing.T) {
	testCase := []struct {
		desc      string
		opt       []Option
		param     ParamPaginationInt64
		expClause string
	}{
		{
			desc: "Query builder with extra limit option",
			opt:  []Option{WithExtraLimit()},
			param: ParamPaginationInt64{
				Page:  1,
				Limit: 10,
			},
			expClause: " WHERE 1=1 LIMIT 0, 11",
		},
	}

	for _, tc := range testCase {
		clause, args, err := New(tc.opt...).Build(&tc.param)
		assert.Nil(t, err)
		assert.Equal(t, tc.expClause, clause)
		assert.Nil(t, args)
	}
}

func Test_ValidatePageAndLimit(t *testing.T) {
	testCase := []struct {
		desc     string
		page     int64
		limit    int64
		expPage  int64
		expLimit int64
	}{
		{
			desc:     "default page=1 and limit=10",
			page:     0,
			limit:    0,
			expPage:  1,
			expLimit: 10,
		},
		{
			desc:     "return original val if page is not zero value",
			page:     2,
			limit:    0,
			expPage:  2,
			expLimit: 10,
		},
		{
			desc:     "return original val if limit is not zero value",
			page:     0,
			limit:    100,
			expPage:  1,
			expLimit: 100,
		},
	}

	for _, tc := range testCase {
		page, limit := ValidatePageAndLimit(tc.page, tc.limit)
		assert.Equal(t, tc.expPage, page)
		assert.Equal(t, tc.expLimit, limit)
	}
}

func Test_QBuilder_WithCustomWhereClause(t *testing.T) {
	t.Run("qbuilder with 1 where clause", func(t *testing.T) {
		param := ParamNull{
			NullString: sql.NullString{Valid: true, String: "hoho"},
		}

		qb := New()
		qb.AddWhereClause("(foo = ? OR bar == ?)", "bar", "foo")
		clause, args, err := qb.Build(&param)
		assert.Nil(t, err)
		assert.Equal(t, " WHERE 1=1 AND nullstring LIKE ? AND (foo = ? OR bar == ?) LIMIT 0, 10", clause)
		assert.Equal(t, []interface{}{"hoho", "bar", "foo"}, args)
	})

	t.Run("qbuilder with >1 where clause", func(t *testing.T) {
		param := ParamNull{
			NullString: sql.NullString{Valid: true, String: "hoho"},
		}

		qb := New()
		qb.AddWhereClause("(foo = ? OR bar == ?)", "bar", "foo")
		qb.AddWhereClause("ping = ?", "pong")
		clause, args, err := qb.Build(&param)
		assert.Nil(t, err)
		assert.Equal(t, " WHERE 1=1 AND nullstring LIKE ? AND (foo = ? OR bar == ?) AND ping = ? LIMIT 0, 10", clause)
		assert.Equal(t, []interface{}{"hoho", "bar", "foo", "pong"}, args)
	})
}
