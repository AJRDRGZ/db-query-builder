package postgres

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/AJRDRGZ/db-query-builder/models"
)

const ErrFieldsAreEmpty = "FAILED! YOU NEED TO SEND FIELDS"

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

// RowScanner utilidad para leer los registros de un Query
type RowScanner interface {
	Scan(dest ...interface{}) error
}

// BuildSQLInsert builds a query INSERT of postgres
func BuildSQLInsert(table string, fields []string) string {
	if len(fields) == 0 {
		return ErrFieldsAreEmpty
	}

	args := bytes.Buffer{}
	values := bytes.Buffer{}

	for k, v := range fields {
		args.WriteString(v)
		args.WriteString(", ")
		values.WriteString(fmt.Sprintf("$%d, ", k+1))
	}

	args.Truncate(args.Len() - 2)
	values.Truncate(values.Len() - 2)

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id, created_at", table, args.String(), values.String())
}

// BuildSQLInsertWithID builds a query INSERT of postgres allowing to send the ID
func BuildSQLInsertWithID(table string, fields []string) string {
	if len(fields) == 0 {
		return ErrFieldsAreEmpty
	}

	args := bytes.Buffer{}
	values := bytes.Buffer{}
	k := 1

	args.WriteString("id, ")
	values.WriteString(fmt.Sprintf("$%d, ", k))

	for _, v := range fields {
		k++
		args.WriteString(v)
		args.WriteString(", ")
		values.WriteString(fmt.Sprintf("$%d, ", k))
	}

	args.Truncate(args.Len() - 2)
	values.Truncate(values.Len() - 2)

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING created_at", table, args.String(), values.String())
}

// BuildSQLUpdateByID builds a query UPDATE of postgres
func BuildSQLUpdateByID(table string, fields []string) string {
	if len(fields) == 0 {
		return ErrFieldsAreEmpty
	}

	args := bytes.Buffer{}
	for k, v := range fields {
		args.WriteString(fmt.Sprintf("%s = $%d, ", v, k+1))
	}

	return fmt.Sprintf("UPDATE %s SET %supdated_at = now() WHERE id = $%d", table, args.String(), len(fields)+1)
}

// BuildSQLSelect builds a query SELECT of postgres
func BuildSQLSelect(table string, fields []string) string {
	if len(fields) == 0 {
		return ErrFieldsAreEmpty
	}

	args := bytes.Buffer{}
	for _, v := range fields {
		args.WriteString(fmt.Sprintf("%s, ", v))
	}

	return fmt.Sprintf("SELECT id, %screated_at, updated_at FROM %s", args.String(), table)
}

// BuildSQLSelectFields builds a query SELECT of postgres
func BuildSQLSelectFields(table string, fields []string) string {
	if len(fields) == 0 {
		return ErrFieldsAreEmpty
	}

	args := bytes.Buffer{}
	for _, v := range fields {
		args.WriteString(fmt.Sprintf("%s, ", v))
	}
	args.Truncate(args.Len() - 2)

	return fmt.Sprintf("SELECT %s FROM %s", args.String(), table)
}

// BuildSQLWhere builds and returns a query WHERE of postgres and its arguments
func BuildSQLWhere(fields models.Fields) (string, []interface{}) {
	if fields.IsEmpty() {
		return ErrFieldsAreEmpty, nil
	}

	query := bytes.Buffer{}
	query.WriteString("WHERE ")
	length := len(fields)
	lastFieldIndex := length - 1
	nGroups := 0
	var args []interface{}

	paramSequence := 1
	for key, field := range fields {
		setDefaultValuesField(&field)

		if field.GroupOpen {
			nGroups++
		}

		switch field.Operator {
		case models.In:
			query.WriteString(BuildIN(field))
		case models.IsNull, models.IsNotNull:
			query.WriteString(fmt.Sprintf("%s %s", strings.ToLower(field.Name), field.Operator))
		case models.Between:
			// TODO: improve this function to return an error instead of string
			if err := field.ValidateFromAndToValues(); err != nil {
				return err.Error(), nil
			}

			query.WriteString(fmt.Sprintf("%s %s $%d AND $%d",
				strings.ToLower(field.Name),
				field.Operator,
				paramSequence,
				paramSequence+1,
			))

			// Increment paramSequence because `BETWEEN` has 2 params always
			paramSequence++
		default:
			// if we need to compare against the column of other table
			if field.IsValueFromTable {
				query.WriteString(fmt.Sprintf("%s %s %s",
					strings.ToLower(field.Name),
					field.Operator,
					strings.ToLower(field.NameValueFromTable),
				))

				break
			}

			// if we compare against a value that we define
			query.WriteString(fmt.Sprintf("%s %s $%d",
				strings.ToLower(field.Name),
				field.Operator,
				paramSequence,
			))
		}

		// Close the group
		if (nGroups > 0) && field.GroupClose {
			nGroups--
			query.WriteString(")")
		}

		// if exists still groups open, close them in the last field
		if (nGroups > 0) && (key == lastFieldIndex) {
			query.WriteString(strings.Repeat(")", nGroups))
		}

		// Add chainingKey (OR, AND) except in the last field
		if key != lastFieldIndex {
			query.WriteString(fmt.Sprintf(" %s ", field.ChainingKey))
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

	return query.String(), args
}

// BuildSQLOrderBy builds and returns a query ORDER BY of postgres and its arguments
func BuildSQLOrderBy(sorts models.SortFields) string {
	if sorts.IsEmpty() {
		return ""
	}

	query := bytes.Buffer{}
	query.WriteString("ORDER BY ")

	for _, sort := range sorts {
		setSortFieldOrder(&sort)
		setSortFieldAliases(&sort)
		query.WriteString(fmt.Sprintf("%s %s, ",
			strings.ToLower(sort.Name),
			sort.Order,
		))
	}
	query.Truncate(query.Len() - 2)

	return query.String()
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
	columns := bytes.Buffer{}
	for _, v := range fields {
		columns.WriteString(fmt.Sprintf("%s.%s, ", aliased, v))
	}

	return fmt.Sprintf("%s.id, %s%s.created_at, %s.updated_at",
		aliased, columns.String(), aliased, aliased)
}

// ColumnsAliasedWithDefault return the column names with aliased of the table
func ColumnsAliasedWithDefault(fields []string, aliased string) string {
	if len(fields) == 0 {
		return ""
	}

	columns := bytes.Buffer{}
	for _, v := range fields {
		columns.WriteString(fmt.Sprintf("%s.%s, ", aliased, v))
	}

	return fmt.Sprintf("%s.id, %s%s.created_at, %s.updated_at",
		aliased, columns.String(), aliased, aliased)
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

	args := bytes.Buffer{}
	switch items := field.Value.(type) {
	case []uint:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args.WriteString(fmt.Sprintf("%d,", item))
		}

		return fmt.Sprintf("%s IN (%s)", nameField, strings.TrimSuffix(args.String(), ","))
	case []int:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args.WriteString(fmt.Sprintf("%d,", item))
		}

		return fmt.Sprintf("%s IN (%s)", nameField, strings.TrimSuffix(args.String(), ","))
	case []string:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args.WriteString(fmt.Sprintf("'%s',", item))
		}

		return fmt.Sprintf("%s IN (%s)", nameField, strings.TrimSuffix(args.String(), ","))
	default:
		return mistakeIN
	}
}

func setDefaultValuesField(field *models.Field) {
	setChainingField(field)
	setOperatorField(field)
	setAliases(field)
	setGroupOpen(field)
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
