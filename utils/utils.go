package utils

import (
	"github.com/jmoiron/sqlx"
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
	"database/sql"
	"errors"
)

func GetTimeStamp() string {
	// Seconds end at 19th character, ignore the rest
	return fmt.Sprintf("%.19s", time.Now().UTC())
}

func GetNull() sql.NullString {
	return sql.NullString{String: "", Valid: false}
}

func GetNullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: true}
}

// TODO Read the discussion about MapScan at link below
// https://github.com/jmoiron/sqlx/issues/135
func MapBytesToString(m map[string]interface{}) {
	for k, v := range m {
		if b, ok := v.([]byte); ok {
			m[k] = string(b)
		}
	}
}

func MapScan(r sqlx.ColScanner, destination map[string]interface{}) error {
	err := sqlx.MapScan(r, destination)
	if err != nil {
		return err
	}
	MapBytesToString(destination)
	return nil
}

// GetBytes converts an arbitrary interface to a byte array
// http://stackoverflow.com/a/23004209/639133
func GetBytes(key interface{}) ([]byte, error) {
	if key == nil {
		return nil, errors.New("Key is nil")
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
