package models

import (
	"errors"
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

// OrderField is the keyword for order the field
type OrderField string

// OrderFields
var (
	Asc  OrderField = "ASC"
	Desc OrderField = "DESC"
)
