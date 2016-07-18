package utils

import (
	"github.com/jmoiron/sqlx"
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
	"database/sql"
	"errors"
	"encoding/json"
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

// JsonString can be used to decode any json value to string
type JsonString string
type jsonString JsonString
func (st *JsonString) UnmarshalJSON(bArr []byte) (err error) {
	j, n, f, b := jsonString(""), uint64(0), float64(0), bool(false)
	if err = json.Unmarshal(bArr, &j); err == nil {
		*st = JsonString(j)
		return
	}
	if err = json.Unmarshal(bArr, &n); err == nil {
		*st = JsonString(string(bArr[:]))
		return
	}
	if err = json.Unmarshal(bArr, &f); err == nil {
		*st = JsonString(string(bArr[:]))
		return
	}
	if err = json.Unmarshal(bArr, &b); err == nil {
		*st = JsonString(string(bArr[:]))
		return
	}
	return
}

// JsonDump can be used to pretty print a struct representing json data
func JsonDump(i interface{}, indent bool) string {
	var indentString string
	if indent {
		indentString = "    "
	} else {
		indentString = ""
	}
	s, _ := json.MarshalIndent(i, "", indentString)
	return string(s)
}

