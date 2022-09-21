package models

import (
	"fmt"
	"strings"
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
		isAllowed := false
		for _, allowedField := range allowedFields {
			if strings.EqualFold(allowedField, field.Name) {
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
