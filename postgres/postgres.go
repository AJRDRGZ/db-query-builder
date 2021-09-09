package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/AJRDRGZ/db-query-builder/models"
	"github.com/lib/pq"
)

// Constraints is a map with a key with the constraint name and contains a value as error
type Constraints map[string]error

// CheckConstraint checks if a error is a psqlError and returns the custom constraint error
func CheckConstraint(constraints Constraints, err error) error {
	psqlErr := &pq.Error{}
	if !errors.As(err, &psqlErr) {
		return err
	}

	constraintErr, ok := constraints[psqlErr.Constraint]
	if !ok {
		return err
	}

	return constraintErr
}

// BuildSQLInsert builds a query INSERT of postgres
func BuildSQLInsert(table string, fields []string) string {
	var args, vals string

	for k, v := range fields {
		args += fmt.Sprintf("%s,", v)
		vals += fmt.Sprintf("$%d,", k+1)
	}

	if len(fields) > 0 {
		args = args[:len(args)-1]
		vals = vals[:len(vals)-1]
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id, created_at", table, args, vals)
}

// BuildSQLUpdateByID builds a query UPDATE of postgres
func BuildSQLUpdateByID(table string, fields []string) string {
	if len(fields) == 0 {
		return ""
	}

	var args string
	for k, v := range fields {
		args += fmt.Sprintf("%s = $%d, ", v, k+1)
	}

	return fmt.Sprintf("UPDATE %s SET %supdated_at = now() WHERE id = $%d", table, args, len(fields)+1)
}

// BuildSQLSelect builds a query SELECT of postgres
func BuildSQLSelect(table string, fields []string) string {
	if len(fields) == 0 {
		return ""
	}

	var args string
	for _, v := range fields {
		args += fmt.Sprintf("%s, ", v)
	}

	return fmt.Sprintf("SELECT id, %screated_at, updated_at FROM %s", args, table)
}

// BuildSQLSelectFields builds a query SELECT of postgres
func BuildSQLSelectFields(table string, fields []string) string {
	if len(fields) == 0 {
		return ""
	}

	var args string
	for _, v := range fields {
		args += fmt.Sprintf("%s, ", v)
	}

	return fmt.Sprintf("SELECT %s FROM %s", args[:len(args)-2], table)
}

// BuildSQLWhere builds and returns a query WHERE of postgres and its arguments
func BuildSQLWhere(fields models.Fields) (string, []interface{}) {
	if fields.IsEmpty() {
		return "", []interface{}{}
	}

	query := "WHERE"
	length := len(fields)
	lastFieldIndex := length - 1
	nGroups := 0
	var args []interface{}

	paramSequence := 1
	for key, field := range fields {
		setChainingField(&field)
		setOperatorField(&field)
		setAliases(&field)
		setGroupOpen(&field)

		if field.GroupOpen {
			nGroups++
		}

		switch field.Operator {
		case models.In:
			query = fmt.Sprintf("%s %s", query, BuildIN(field))
		case models.IsNull, models.IsNotNull:
			query = fmt.Sprintf("%s %s %s",
				query,
				strings.ToLower(field.Name),
				field.Operator,
			)
		case models.Between:
			// TODO: improve this function to return an error instead of string
			if err := field.ValidateFromAndToValues(); err != nil {
				return err.Error(), nil
			}

			query = fmt.Sprintf("%s %s %s $%d AND $%d",
				query,
				strings.ToLower(field.Name),
				field.Operator,
				paramSequence,
				paramSequence+1,
			)

			// Increment paramSequence because `BETWEEN` has 2 params always
			paramSequence++
		default:
			// if we need to compare against the column of other table
			if field.IsValueFromTable {
				query = fmt.Sprintf("%s %s %s %s",
					query,
					strings.ToLower(field.Name),
					field.Operator,
					strings.ToLower(field.NameValueFromTable),
				)

				break
			}

			// if we compare against a value that we define
			query = fmt.Sprintf("%s %s %s $%d",
				query,
				strings.ToLower(field.Name),
				field.Operator,
				paramSequence,
			)
		}

		// Close the group
		if (nGroups > 0) && field.GroupClose {
			nGroups--
			query += ")"
		}

		// if exists still groups open, close them in the last field
		if (nGroups > 0) && (key == lastFieldIndex) {
			query += strings.Repeat(")", nGroups)
		}

		// Add chainingKey (OR, AND) except in the last field
		if key != lastFieldIndex {
			query = fmt.Sprintf("%s %s", query, field.ChainingKey)
		}

		if field.Operator == models.In ||
			field.Operator == models.IsNull ||
			field.Operator == models.IsNotNull ||
			field.IsValueFromTable {

			continue
		}

		// Add arguments of the parameters when operator is different to "IN, IsNull, IsNotNull" or when IsValueFromTable is true
		if field.Value != nil {
			args = append(args, field.Value)
		}
		if field.Operator == models.Between {
			args = append(args, field.FromValue, field.ToValue)
		}

		paramSequence++
	}

	return query, args
}

// BuildSQLOrderBy builds and returns a query ORDER BY of postgres and its arguments
func BuildSQLOrderBy(sorts models.SortFields) string {
	if sorts.IsEmpty() {
		return ""
	}

	query, length := "ORDER BY", len(sorts)

	for key, sort := range sorts {
		setSortFieldOrder(&sort)
		setSortFieldAliases(&sort)
		query = fmt.Sprintf("%s %s %s",
			query,
			strings.ToLower(sort.Name),
			sort.Order,
		)
		if key != (length - 1) {
			query = fmt.Sprintf("%s,", query)
		}
	}

	return query
}

// BuildSQLPagination builds and returns a query OFFSET LIMIT of postgres for pagination
func BuildSQLPagination(pag models.Pagination) string {
	if pag.Limit == 0 && pag.Page == 0 {
		return ""
	}
	if pag.MaxLimit == 0 {
		pag.MaxLimit = 20
	}

	if pag.Limit == 0 || pag.Limit > pag.MaxLimit {
		pag.Limit = pag.MaxLimit
	}
	if pag.Page == 0 {
		pag.Page = 1
	}

	offset := pag.Page*pag.Limit - pag.Limit

	pagination := fmt.Sprintf("LIMIT %d OFFSET %d", pag.Limit, offset)

	return pagination
}

// ColumnsAliased return the column names with aliased of the table
func ColumnsAliased(fields []string, aliased string) string {
	if len(fields) == 0 {
		return ""
	}
	columns := ""
	for _, v := range fields {
		columns += fmt.Sprintf("%s.%s, ", aliased, v)
	}

	return fmt.Sprintf("%s.id, %s%s.created_at, %s.updated_at",
		aliased, columns, aliased, aliased)
}

// ColumnsAliasedWithDefault return the column names with aliased of the table
func ColumnsAliasedWithDefault(fields []string, aliased string) string {
	if len(fields) == 0 {
		return ""
	}
	columns := ""
	for _, v := range fields {
		columns += fmt.Sprintf("%s.%s, ", aliased, v)
	}

	return fmt.Sprintf("%s.id, %s%s.created_at, %s.updated_at",
		aliased, columns, aliased, aliased)
}

// CheckError validate a postgres error
func CheckError(err error) error {
	if psqlErr, ok := err.(*pq.Error); ok {
		switch psqlErr.Code {
		case "23505":
			return models.ErrUnique
		case "23503":
			return models.ErrForeignKey
		case "23502":
			return models.ErrNotNull
		}
	}
	return nil
}

func BuildIN(field models.Field) string {
	nameField := strings.ToLower(field.Name)
	// if the IN failed, return mistakeIN for not select nothing in the field
	mistakeIN := fmt.Sprintf("%s = 0", nameField)

	var args string
	switch items := field.Value.(type) {
	case []uint:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args += fmt.Sprintf("%d,", item)
		}

		return fmt.Sprintf("%s IN (%s)", nameField, strings.TrimSuffix(args, ","))
	case []int:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args += fmt.Sprintf("%d,", item)
		}

		return fmt.Sprintf("%s IN (%s)", nameField, strings.TrimSuffix(args, ","))
	case []string:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args += fmt.Sprintf("'%s',", item)
		}

		return fmt.Sprintf("%s IN (%s)", nameField, strings.TrimSuffix(args, ","))
	default:
		return mistakeIN
	}
}

func setChainingField(field *models.Field) {
	if field.ChainingKey == "" {
		field.ChainingKey = models.And
	}
}

func setOperatorField(field *models.Field) {
	if field.Operator == "" {
		field.Operator = models.Equals
	}
}

func setAliases(field *models.Field) {
	if field.Source != "" {
		field.Name = fmt.Sprintf("%s.%s", field.Source, field.Name)
	}

	if field.SourceNameValueFromTable != "" {
		field.NameValueFromTable = fmt.Sprintf("%s.%s", field.SourceNameValueFromTable, field.NameValueFromTable)
	}
}

func setGroupOpen(field *models.Field) {
	if field.GroupOpen {
		field.Name = fmt.Sprintf("(%s", field.Name)
	}
}

func setSortFieldOrder(sortField *models.SortField) {
	if sortField.Order == "" {
		sortField.Order = models.Asc
	}
}

func setSortFieldAliases(sortField *models.SortField) {
	if sortField.Source != "" {
		sortField.Name = fmt.Sprintf("%s.%s", sortField.Source, sortField.Name)
	}
}
