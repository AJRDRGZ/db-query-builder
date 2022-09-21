package models

import (
	"fmt"
	"reflect"
	"strings"
)

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
		isAllowed := false
		for _, allowedField := range allowedFields {
			if strings.EqualFold(allowedField, field.Name) {
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
		isAllowed := false
		for _, sourceAllowed := range sourcesAllowed {
			if strings.EqualFold(sourceAllowed, field.Source) {
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
		if strings.EqualFold(field.Name, inputField) {
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
