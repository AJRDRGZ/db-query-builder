package nullhandler

import (
	"database/sql"
	"time"
)

// TimeToNull devuelve una estructura nil si la fecha está en valor (zero)
func TimeToNull(t time.Time) sql.NullTime {
	r := sql.NullTime{}
	r.Time = t

	if !t.IsZero() {
		r.Valid = true
	}

	return r
}

// ParseDateToTime devuelve una estructura nil si la hora está en valor (zero)
func ParseDateToTime(s string) sql.NullTime {
	format := "15:04:05"
	t, _ := time.Parse(format, s)

	return TimeToNull(t)
}

// Int64ToNull devuelve una estructura nil si el entero es (zero)
func Int64ToNull(i int64) sql.NullInt64 {
	r := sql.NullInt64{}
	r.Int64 = i

	if i > 0 {
		r.Valid = true
	}

	return r
}

// StringToNull devuelve una estructura nil si la cadena de texto está vacia
func StringToNull(s string) sql.NullString {
	r := sql.NullString{}
	r.String = s

	if s != "" {
		r.Valid = true
	}

	return r
}

// Float64ToNull devuelve una estructura nil si el valor es cero
func Float64ToNull(f float64) sql.NullFloat64 {
	r := sql.NullFloat64{}
	r.Float64 = f

	if f > 0 {
		r.Valid = true
	}

	return r
}

// BoolToNull devuelve una estructura nil si el puntero al booleano es nil.
// Sólo funciona con punteros a bool.
func BoolToNull(b *bool) sql.NullBool {
	r := sql.NullBool{}

	if b != nil {
		r.Bool = *b
		r.Valid = true
	}

	return r
}
