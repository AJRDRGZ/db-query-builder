package models

// FieldsSpecification is the specification for an implementation of an infrastructure,
// see the repository pattern:
// https://thinkinginobjects.com/2012/08/26/dont-use-dao-use-repository/
type FieldsSpecification struct {
	Filters    Fields
	Sorts      SortFields
	Pagination Pagination
}
