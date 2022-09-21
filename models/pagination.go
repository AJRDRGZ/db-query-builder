package models

// Pagination contains the information of the pagination
type Pagination struct {
	Page     uint `json:"page"`
	Limit    uint `json:"limit"`
	MaxLimit uint
}
