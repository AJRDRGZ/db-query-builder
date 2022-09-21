package nullhandler

import (
	"database/sql"
	"fmt"
	"time"
)

// ExecAffectingOneRow ejecuta una sentencia (statement),
// esperando una sola fila afectada.
func ExecAffectingOneRow(stmt *sql.Stmt, args ...interface{}) error {
	r, err := stmt.Exec(args...)
	if err != nil {
		return fmt.Errorf("psql: could not execute statement %w", err)
	}

	rowsAffected, err := r.RowsAffected()
	if err != nil {
		return fmt.Errorf("psql: could not get rows affected %w", err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("psql: expected 1 row affected, got %d", rowsAffected)
	}

	return nil
}

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
