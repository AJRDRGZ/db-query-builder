package models

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	ErrInvalidPaginationParameter  = errors.New("invalid pagination parameters")
	ErrFromValueIsEmpty            = errors.New("`from` value is empty")
	ErrToValueIsEmpty              = errors.New("`to` value is empty")
	ErrFromAndToValuesAreMissMatch = errors.New("`from` and `to` values are missmatch")
)

// Errors SQL
var (
	ErrUnique     = errors.New("Unique violation")
	ErrForeignKey = errors.New("Foreign key violation")
	ErrNotNull    = errors.New("Not Null violation")
)

type operatorField string

// Operators
const (
	Equals               operatorField = "="
	NotEqualTo           operatorField = "<>"
	LessThan             operatorField = "<"
	GreaterThan          operatorField = ">"
	LessThanOrEqualTo    operatorField = "<="
	GreaterThanOrEqualTo operatorField = ">="
	Ilike                operatorField = "ILIKE"
	In                   operatorField = "IN"
	IsNull               operatorField = "IS NULL"
	IsNotNull            operatorField = "IS NOT NULL"
	Between              operatorField = "BETWEEN"
)

// ChainingField is the keyword for chaining the next field
type ChainingField string

// Chaining
const (
	And ChainingField = "AND"
	Or  ChainingField = "OR"
)

// FieldsSpecification is the specification for an implementation of an infrastructure,
// see the repository pattern:
// https://thinkinginobjects.com/2012/08/26/dont-use-dao-use-repository/
type FieldsSpecification struct {
	Filters    Fields
	Sorts      SortFields
	Pagination Pagination
}

// Field contains the information of a field for a query
type Field struct {
	Name     string        `json:"name"`
	Operator operatorField `json:"operator"`
	Value    interface{}   `json:"value"`

	// FromValue and ToValue are used ONLY for `Between` structure
	FromValue interface{} `json:"from_value"`
	ToValue   interface{} `json:"to_value"`

	// ChainingKey is the operator `and` or `or`
	ChainingKey ChainingField `json:"chaining_key"`
	// Source sets the origin of the field, is used if a resource has more of one source,
	// this is useful generally when an infrastructure implementation used "Joins"
	Source string `json:"source"` // Optional
	// GroupOpen allows beginning a conditions group of fields and the infrastructure
	// implementation must include into group the field that sets the GroupOpen = true
	GroupOpen bool `json:"group_open"` // Optional
	// GroupClose allows ending a conditions group of fields, and the infrastructure
	// implementation must include into group the field that sets the GroupClose = true
	GroupClose bool `json:"group_close"` // Optional

	// IsValueFromTable allows to compare the value from Name with the value of other table
	// ej: un.ends_at >= pp.ends_at
	// to implement, this field must be true
	IsValueFromTable bool `json:"is_value_from_table"` // Optional
	// NameValueFromTable indicates a value with a column of a table instead of
	// using the Value field to specify the concrete value
	NameValueFromTable string
	// SourceNameValueFromTable sets the origin of the NameValueFromTable
	// is used if a resource has mor of one source and IsValueFromTable is true
	SourceNameValueFromTable string `json:"source_name_value_from_table"`
}

// ValidateFromAndToValues returns if `from` and `to` values are valid
func (f Field) ValidateFromAndToValues() error {
	if f.FromValue == nil {
		return ErrFromValueIsEmpty
	}
	if f.ToValue == nil {
		return ErrToValueIsEmpty
	}

	if reflect.TypeOf(f.FromValue) != reflect.TypeOf(f.ToValue) {
		return ErrFromAndToValuesAreMissMatch
	}

	return nil
}

// Fields slice of Field
type Fields []Field

// IsEmpty returns if the Fields is empty
func (fs Fields) IsEmpty() bool { return len(fs) == 0 }

// Push add one or more Field in the slice
func (fs *Fields) Push(f ...Field) {
	*fs = append(*fs, f...)
}

// ValidateNames validates if the fields is allowed for query
func (fs Fields) ValidateNames(allowedFields []string) error {
	for _, field := range fs {
		var isAllowed bool
		for _, allowedField := range allowedFields {
			if strings.ToLower(allowedField) == strings.ToLower(field.Name) {
				isAllowed = true
				break
			}
		}
		if !isAllowed {
			return fmt.Errorf("the field %s is not allowed for query", field.Name)
		}
	}
	return nil
}

// ValidateSources validates if the fields source is allowed for query
func (fs Fields) ValidateSources(sourcesAllowed []string) error {
	for _, field := range fs {
		var isAllowed bool
		for _, SourceAllowed := range sourcesAllowed {
			if strings.ToLower(SourceAllowed) == strings.ToLower(field.Source) {
				isAllowed = true
				break
			}
		}
		if !isAllowed {
			return fmt.Errorf("the source %s is not allowed for query", field.Source)
		}
	}
	return nil
}

// FindField returns the Field, and it returns if field was found
func (fs Fields) FindField(inputField string) (Field, bool) {
	for _, field := range fs {
		if strings.ToLower(field.Name) == strings.ToLower(inputField) {
			return field, true
		}
	}
	return Field{}, false
}

// Error returns an error string with the field name and value
func (fs Fields) Error() string {
	err := "not found"
	if len(fs) == 0 {
		return err
	}
	for _, field := range fs {
		err = fmt.Sprintf("%s: %v, %s", field.Name, field.Value, err)
	}

	return err
}

// OrderField is the keyword for order the field
type OrderField string

// OrderFields
var (
	Asc  OrderField = "ASC"
	Desc OrderField = "DESC"
)

// SortField contains the information of the order of a field
type SortField struct {
	Name  string     `json:"name"`
	Order OrderField `json:"order"`
	// Source sets the origin of the field, is used if a resource has more of one source,
	// this is useful generally when an infrastructure implementation used "Joins"
	Source string `json:"source"` // Optional
}

// SortFields slice of SortField
type SortFields []SortField

// IsEmpty returns if the SortFields is empty
func (ss SortFields) IsEmpty() bool { return len(ss) == 0 }

// ValidateNames valida if the fields is allowed for ordering
func (ss SortFields) ValidateNames(allowedFields []string) error {
	for _, field := range ss {
		var isAllowed bool
		for _, allowedField := range allowedFields {
			if strings.ToLower(allowedField) == strings.ToLower(field.Name) {
				isAllowed = true
				break
			}
		}
		if !isAllowed {
			return fmt.Errorf("the field %s is not allowed for ordering", field.Name)
		}
	}
	return nil
}

// Pagination contains the information of the pagination
type Pagination struct {
	Page     uint `json:"page"`
	Limit    uint `json:"limit"`
	MaxLimit uint
}
