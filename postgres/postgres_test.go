package postgres

import (
	"testing"
	"time"

	"github.com/AJRDRGZ/db-query-builder/models"

	"github.com/stretchr/testify/assert"
)

func TestBuildSQLInsert(t *testing.T) {
	tableTest := []struct {
		table  string
		fields []string
		want   string
	}{
		{
			table:  "cashboxes",
			fields: []string{"responsable", "country", "user_id", "account"},
			want:   "INSERT INTO cashboxes (responsable, country, user_id, account) VALUES ($1, $2, $3, $4) RETURNING id, created_at",
		},
		{
			table:  "one",
			fields: []string{"one_field"},
			want:   "INSERT INTO one (one_field) VALUES ($1) RETURNING id, created_at",
		},
		{
			table:  "empty",
			fields: []string{},
			want:   ErrFieldsAreEmpty,
		},
	}

	for _, tt := range tableTest {
		assert.Equal(t, tt.want, BuildSQLInsert(tt.table, tt.fields))
	}
}

func TestBuildSQLInsertWithID(t *testing.T) {
	tableTest := []struct {
		table  string
		fields []string
		want   string
	}{
		{
			table:  "cashboxes",
			fields: []string{"responsable", "country", "user_id", "account"},
			want:   "INSERT INTO cashboxes (id, responsable, country, user_id, account) VALUES ($1, $2, $3, $4, $5) RETURNING created_at",
		},
		{
			table:  "one",
			fields: []string{"one_field"},
			want:   "INSERT INTO one (id, one_field) VALUES ($1, $2) RETURNING created_at",
		},
		{
			table:  "empty",
			fields: []string{},
			want:   ErrFieldsAreEmpty,
		},
	}

	for _, tt := range tableTest {
		assert.Equal(t, tt.want, BuildSQLInsertWithID(tt.table, tt.fields))
	}
}

func TestBuildSQLUpdateByID(t *testing.T) {
	tableTest := []struct {
		table  string
		fields []string
		want   string
	}{
		{
			table:  "cashboxes",
			fields: []string{"responsable", "country", "user_id", "account"},
			want:   "UPDATE cashboxes SET responsable = $1, country = $2, user_id = $3, account = $4, updated_at = now() WHERE id = $5",
		},
		{
			table:  "nothing",
			fields: []string{},
			want:   ErrFieldsAreEmpty,
		},
		{
			table:  "one",
			fields: []string{"one_field"},
			want:   "UPDATE one SET one_field = $1, updated_at = now() WHERE id = $2",
		},
	}

	for _, tt := range tableTest {
		assert.Equal(t, tt.want, BuildSQLUpdateByID(tt.table, tt.fields))
	}
}

func TestBuildSQLSelect(t *testing.T) {
	tableTest := []struct {
		table  string
		fields []string
		want   string
	}{
		{
			table:  "cashboxes",
			fields: []string{"responsable", "country", "user_id", "account"},
			want:   "SELECT id, responsable, country, user_id, account, created_at, updated_at FROM cashboxes",
		},
		{
			table:  "nothing",
			fields: []string{},
			want:   ErrFieldsAreEmpty,
		},
		{
			table:  "one",
			fields: []string{"one_field"},
			want:   "SELECT id, one_field, created_at, updated_at FROM one",
		},
	}

	for _, tt := range tableTest {
		assert.Equal(t, tt.want, BuildSQLSelect(tt.table, tt.fields))
	}
}

func TestBuildSQLSelectFields(t *testing.T) {
	tableTest := []struct {
		table  string
		fields []string
		want   string
	}{
		{
			table:  "cashboxes",
			fields: []string{"responsable", "country", "user_id", "account"},
			want:   "SELECT responsable, country, user_id, account FROM cashboxes",
		},
		{
			table:  "nothing",
			fields: []string{},
			want:   ErrFieldsAreEmpty,
		},
		{
			table:  "one",
			fields: []string{"one_field"},
			want:   "SELECT one_field FROM one",
		},
	}

	for _, tt := range tableTest {
		assert.Equal(t, tt.want, BuildSQLSelectFields(tt.table, tt.fields))
	}
}

func TestBuildSQLWhere(t *testing.T) {
	fakeDate := time.Date(2021, 4, 28, 0, 0, 0, 0, time.UTC).Format("2006-01-02")

	tableTest := []struct {
		name      string
		fields    models.Fields
		wantQuery string
		wantArgs  []interface{}
	}{
		{
			name:      "where with emtpy fields",
			fields:    models.Fields{},
			wantQuery: ErrFieldsAreEmpty,
			wantArgs:  nil,
		},
		{
			name: "where with ILIKE",
			fields: models.Fields{
				{Name: "id", Value: []uint{1, 2, 3}, Operator: models.In},
			},
			wantQuery: "WHERE id IN (1,2,3)",
			wantArgs:  nil,
		},
		{
			name: "where with all operators",
			fields: models.Fields{
				{Name: "name", Value: "Alejandro"},
				{Name: "age", Value: 30, ChainingKey: models.Or},
				{Name: "course", Value: "Go"},
				{Name: "id", Value: []uint{1, 4, 9}, Operator: models.In},
				{Name: "DESCRIPTION", Value: "%golang%", Operator: models.Ilike},
				{Name: "certificates", Value: 3, Operator: models.GreaterThan},
				{Name: "is_active", Value: true},
			},
			wantQuery: "WHERE name = $1 AND age = $2 OR course = $3 AND id IN (1,4,9) AND description ILIKE $4 AND certificates > $5 AND is_active = $6",
			wantArgs:  []interface{}{"Alejandro", 30, "Go", "%golang%", 3, true},
		},
		{
			name: "where with operators and string ILIKE",
			fields: models.Fields{
				{Name: "country", Value: "COLOMBIA"},
				{Name: "currency_id", Value: 3, ChainingKey: models.Or},
				{Name: "enable", Value: true},
				{Name: "code", Value: []string{"COL", "COP"}, Operator: models.In},
			},
			wantQuery: "WHERE country = $1 AND currency_id = $2 OR enable = $3 AND code IN ('COL','COP')",
			wantArgs:  []interface{}{"COLOMBIA", 3, true},
		},
		{
			name: "where with operators and NOT NULL",
			fields: models.Fields{
				{Name: "country", Value: "COLOMBIA"},
				{Name: "currency_id", Value: 3, ChainingKey: models.Or},
				{Name: "begins_at", Value: "fake", Operator: models.IsNull},
				{Name: "enable", Value: true},
				{Name: "code", Value: []string{"COL", "COP"}, Operator: models.In},
			},
			wantQuery: "WHERE country = $1 AND currency_id = $2 OR begins_at IS NULL AND enable = $3 AND code IN ('COL','COP')",
			wantArgs:  []interface{}{"COLOMBIA", 3, true},
		},
		{
			name: "where with aliased",
			fields: models.Fields{
				{Source: "contracts", Name: "employer_id", Value: 777},
				{Source: "contracts", Name: "pay_frequency_id", Value: 2, ChainingKey: models.Or},
				{Source: "contracts", Name: "is_active", Value: true},
				{Source: "contract_statuses", Name: "description", Value: "ACTIVE", Operator: models.Ilike},
			},
			wantQuery: "WHERE contracts.employer_id = $1 AND contracts.pay_frequency_id = $2 OR contracts.is_active = $3 AND contract_statuses.description ILIKE $4",
			wantArgs:  []interface{}{777, 2, true, "ACTIVE"},
		},
		{
			name: "where with aliased on two tables with upper case",
			fields: models.Fields{
				{Source: "contracts", Name: "employer_id", Value: 777},
				{Source: "contracts", Name: "pay_frequency_id", Value: 2, ChainingKey: models.Or},
				{Source: "contracts", Name: "endS_at", Operator: models.LessThan, IsValueFromTable: true, SourceNameValueFromTable: "peRiods", NameValueFromTable: "eNds_at"},
				{Source: "contracts", Name: "is_active", Value: true},
				{Source: "contract_statuses", Name: "description", Value: "ACTIVE", Operator: models.Ilike},
			},
			wantQuery: "WHERE contracts.employer_id = $1 AND contracts.pay_frequency_id = $2 OR contracts.ends_at < periods.ends_at AND contracts.is_active = $3 AND contract_statuses.description ILIKE $4",
			wantArgs:  []interface{}{777, 2, true, "ACTIVE"},
		},
		{
			name: "where with aliased where some fields have missing source",
			fields: models.Fields{
				{Name: "employer_id", Value: 19},
				{Name: "pay_frequency_id", Value: 1, ChainingKey: models.Or},
				{Name: "is_active", Value: false},
				{Source: "contract_statuses", Name: "description", Value: "CREATED", Operator: models.Ilike},
			},
			wantQuery: "WHERE employer_id = $1 AND pay_frequency_id = $2 OR is_active = $3 AND contract_statuses.description ILIKE $4",
			wantArgs:  []interface{}{19, 1, false, "CREATED"},
		},
		{
			name: "where with group conditions",
			fields: models.Fields{
				{Name: "employer_id", Value: 1},
				{Name: "pay_frequency_id", Value: 2},
				{GroupOpen: true, Name: "is_active", Value: true, ChainingKey: models.Or},
				{GroupClose: true, Name: "is_staff", Value: false},
				{Source: "contract_statuses", Name: "description", Value: "ACTIVE", Operator: models.Ilike},
			},
			wantQuery: "WHERE employer_id = $1 AND pay_frequency_id = $2 AND (is_active = $3 OR is_staff = $4) AND contract_statuses.description ILIKE $5",
			wantArgs:  []interface{}{1, 2, true, false, "ACTIVE"},
		},
		{
			name: "where with group conditions and with missing GroupClose key",
			fields: models.Fields{
				{Name: "employer_id", Value: 1},
				{Name: "pay_frequency_id", Value: 2},
				{GroupOpen: true, Name: "is_active", Value: true, ChainingKey: models.Or},
				{Source: "contract_statuses", Name: "description", Value: "ACTIVE", Operator: models.Ilike},
			},
			wantQuery: "WHERE employer_id = $1 AND pay_frequency_id = $2 AND (is_active = $3 OR contract_statuses.description ILIKE $4)",
			wantArgs:  []interface{}{1, 2, true, "ACTIVE"},
		},
		{
			name: "where with group conditions and aliases - complex",
			fields: models.Fields{
				{Source: "c", Name: "employer_id", Value: 1},
				{Source: "c", Name: "termination_date", Operator: models.IsNotNull},
				{Source: "c", Name: "pay_frequency_id", Value: 2},
				{GroupOpen: true, Source: "cs", Name: "description", Operator: models.Ilike, Value: "ACTIVE", ChainingKey: models.Or}, {GroupOpen: true, Source: "cs", Name: "description", Operator: models.Ilike, Value: "CREATED"},
				{GroupClose: true, Source: "c", Name: "hire_date", Operator: models.LessThanOrEqualTo, Value: fakeDate},
			},
			wantQuery: "WHERE c.employer_id = $1 AND c.termination_date IS NOT NULL AND c.pay_frequency_id = $2 AND (cs.description ILIKE $3 OR (cs.description ILIKE $4 AND c.hire_date <= $5))",
			wantArgs:  []interface{}{1, 2, "ACTIVE", "CREATED", "2021-04-28"},
		},
		{
			name: "where with group conditions and aliases - complex",
			fields: models.Fields{
				{Source: "c", Name: "employer_id", Value: 1},
				{Source: "c", Name: "ends_at", IsValueFromTable: true, SourceNameValueFromTable: "pp", NameValueFromTable: "ends_at"},
				{Source: "c", Name: "termination_date", Operator: models.IsNotNull},
				{Source: "c", Name: "pay_frequency_id", Value: 2}, {GroupOpen: true, Source: "cs", Name: "description", Operator: models.Ilike, Value: "ACTIVE", ChainingKey: models.Or}, {GroupOpen: true, Source: "cs", Name: "description", Operator: models.Ilike, Value: "CREATED"}, {GroupClose: true, Source: "c", Name: "hire_date", Operator: models.LessThanOrEqualTo, Value: fakeDate}},
			wantQuery: "WHERE c.employer_id = $1 AND c.ends_at = pp.ends_at AND c.termination_date IS NOT NULL AND c.pay_frequency_id = $2 AND (cs.description ILIKE $3 OR (cs.description ILIKE $4 AND c.hire_date <= $5))",
			wantArgs:  []interface{}{1, 2, "ACTIVE", "CREATED", "2021-04-28"}},
		{
			name: "where with group conditions and aliases - complex", fields: models.Fields{
				{Source: "c", Name: "employer_id", Value: 1},
				{Source: "c", Name: "ends_at", IsValueFromTable: true, SourceNameValueFromTable: "pp", NameValueFromTable: "ends_at"},
				{Source: "c", Name: "termination_date", Operator: models.IsNotNull},
				{Source: "c", Name: "pay_frequency_id", Value: 2},
				{GroupOpen: true, Source: "cs", Name: "description", Operator: models.Ilike, Value: "ACTIVE", ChainingKey: models.Or},
				{Source: "c", Name: "frequency", Operator: models.GreaterThanOrEqualTo, IsValueFromTable: true, SourceNameValueFromTable: "s", NameValueFromTable: "months"},
				{GroupOpen: true, Source: "cs", Name: "description", Operator: models.Ilike, Value: "CREATED"},
				{GroupClose: true, Source: "c", Name: "hire_date", Operator: models.LessThanOrEqualTo, Value: fakeDate},
			},
			wantQuery: "WHERE c.employer_id = $1 AND c.ends_at = pp.ends_at AND c.termination_date IS NOT NULL AND c.pay_frequency_id = $2 AND (cs.description ILIKE $3 OR c.frequency >= s.months AND (cs.description ILIKE $4 AND c.hire_date <= $5))",
			wantArgs:  []interface{}{1, 2, "ACTIVE", "CREATED", "2021-04-28"},
		},
		{
			name: "where with BETWEEN",
			fields: models.Fields{
				{Name: "begins_at", Operator: models.Between, FromValue: parseToDate(2010, 5, 3), ToValue: parseToDate(2020, 1, 1)},
			},
			wantQuery: "WHERE begins_at BETWEEN $1 AND $2",
			wantArgs:  []interface{}{parseToDate(2010, 5, 3), parseToDate(2020, 1, 1)},
		},
		{
			name: "where with group conditions and aliases and between - complex",
			fields: models.Fields{
				{Source: "c", Name: "employer_id", Value: 1},
				{Source: "c", Name: "ends_at", IsValueFromTable: true, SourceNameValueFromTable: "pp", NameValueFromTable: "ends_at"},
				{Source: "c", Name: "termination_date", Operator: models.IsNotNull},
				{Source: "c", Name: "pay_frequency_id", Value: 2},
				{GroupOpen: true, Source: "cs", Name: "description", Operator: models.Ilike, Value: "ACTIVE", ChainingKey: models.Or},
				{Source: "c", Name: "frequency", Operator: models.GreaterThanOrEqualTo, IsValueFromTable: true, SourceNameValueFromTable: "s", NameValueFromTable: "months"},
				{Source: "c", Name: "begins_at", Operator: models.Between, FromValue: parseToDate(2020, 1, 1), ToValue: parseToDate(2021, 12, 31)},
				{GroupOpen: true, Source: "cs", Name: "description", Operator: models.Ilike, Value: "CREATED"},
				{GroupClose: true, Source: "c", Name: "hire_date", Operator: models.LessThanOrEqualTo, Value: fakeDate},
			},
			wantQuery: "WHERE c.employer_id = $1 AND c.ends_at = pp.ends_at AND c.termination_date IS NOT NULL AND c.pay_frequency_id = $2 AND (cs.description ILIKE $3 OR c.frequency >= s.months AND c.begins_at BETWEEN $4 AND $5 AND (cs.description ILIKE $6 AND c.hire_date <= $7))",
			wantArgs:  []interface{}{1, 2, "ACTIVE", parseToDate(2020, 1, 1), parseToDate(2021, 12, 31), "CREATED", "2021-04-28"},
		},
	}

	for _, tt := range tableTest {
		gotQuery, gotArgs := BuildSQLWhere(tt.fields)
		assert.Equal(t, tt.wantQuery, gotQuery, tt.name)
		assert.Equal(t, tt.wantArgs, gotArgs, tt.name)
	}
}

func TestColumnsAliased(t *testing.T) {
	tableTest := []struct {
		aliased string
		fields  []string
		want    string
	}{
		{
			aliased: "b",
			fields:  []string{"title", "slug", "content", "poster"},
			want:    "b.id, b.title, b.slug, b.content, b.poster, b.created_at, b.updated_at",
		},
		{
			aliased: "nothing",
			fields:  []string{},
			want:    "",
		},
		{
			aliased: "one",
			fields:  []string{"one_field"},
			want:    "one.id, one.one_field, one.created_at, one.updated_at",
		},
	}

	for _, tt := range tableTest {
		assert.Equal(t, tt.want, ColumnsAliased(tt.fields, tt.aliased))
	}
}

func TestBuildSQLOrderBy(t *testing.T) {
	tests := []struct {
		name  string
		sorts models.SortFields
		want  string
	}{
		{
			name:  "Without sort order specification",
			sorts: models.SortFields{{Name: "id"}, {Name: "begins_at"}},
			want:  "ORDER BY id ASC, begins_at ASC",
		},
		{
			name:  "With sort order specification",
			sorts: models.SortFields{{Name: "id", Order: models.Desc}, {Name: "begins_at", Order: models.Asc}},
			want:  "ORDER BY id DESC, begins_at ASC",
		},
		{
			name:  "With sort alias",
			sorts: models.SortFields{{Name: "id", Source: "a"}, {Name: "begins_at", Source: "b"}},
			want:  "ORDER BY a.id ASC, b.begins_at ASC",
		},
		{
			name:  "One field sort",
			sorts: models.SortFields{{Name: "id"}},
			want:  "ORDER BY id ASC",
		},
		{
			name:  "Without field sorts",
			sorts: models.SortFields{},
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildSQLOrderBy(tt.sorts)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildIN(t *testing.T) {
	tableTest := []struct {
		field     models.Field
		wantQuery string
	}{
		{
			field: models.Field{
				Name: "id", Value: []uint{1, 2, 3}, Operator: models.In,
			},
			wantQuery: "id IN (1,2,3)",
		},
		{
			field: models.Field{
				Name: "employee_id", Value: []int{5, 6, 7}, Operator: models.In,
			},
			wantQuery: "employee_id IN (5,6,7)",
		},
		{
			field: models.Field{
				Name: "marital_status", Value: []string{"SINGLE"}, Operator: models.In,
			},
			wantQuery: "marital_status IN ('SINGLE')",
		},
		{
			field: models.Field{
				Name: "employee_id", Value: "fake", Operator: models.In,
			},
			wantQuery: "employee_id = 0",
		},
		{
			field: models.Field{
				Name: "contract_id", Value: []uint{}, Operator: models.In,
			},
			wantQuery: "contract_id = 0",
		},
	}

	for _, tt := range tableTest {
		gotQuery := BuildIN(tt.field)
		assert.Equal(t, tt.wantQuery, gotQuery)
	}
}

func parseToDate(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func TestBuildSQLPagination(t *testing.T) {
	tests := []struct {
		name string
		args models.Pagination
		want string
	}{
		{
			name: "empty pagination",
			args: models.Pagination{},
			want: "",
		},
		{
			name: "first page",
			args: models.Pagination{
				Page:     0,
				Limit:    5,
				MaxLimit: 0,
			},
			want: "LIMIT 5 OFFSET 0",
		},
		{
			name: "page 2",
			args: models.Pagination{
				Page:     2,
				Limit:    10,
				MaxLimit: 10,
			},
			want: "LIMIT 10 OFFSET 10",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, BuildSQLPagination(tt.args), "BuildSQLPagination(%v)", tt.args)
		})
	}
}
