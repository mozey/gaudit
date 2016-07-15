package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"time"
	"bytes"
	"encoding/gob"
	"errors"
)

var firstRun bool

type Meta struct {
	rowsProcessed   int64
	databaseChanges int64
	tableChanges    int64
	executionTime   time.Duration
}

type HashRow struct {
	Dump []byte
	Hash string
}

type AuditRow struct {
	TableName string `db:"tableName"`
	Hash      string `db:"hash"`
	Dump      sql.NullString `db:"dump"`
	Modified  string `db:"modified"`
}

func getTimeStamp() string {
	// Seconds end at 19th character, ignore the rest
	return fmt.Sprintf("%.19s", time.Now().UTC())
}

func getNull() sql.NullString {
	return sql.NullString{String: "", Valid: false}
}

func getNullString(s string) sql.NullString {
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

// initAudit initialises audit.db the first time `gaudit scan` is executed.
// To reset simply delete audit.db from the filesystem.
// It returns true if the audit table is empty
func initAudit(auditDb *sqlx.DB) (firstRun bool) {
	schema := `
	create table if not exists
		row (tableName, hash, dump, modified);
	create table if not exists
		cell (tableName, columnName, hash, modified);
	create table if not exists
		meta (key, value);
	`
	auditDb.Exec(schema)

	var rowCount int
	err := auditDb.Get(&rowCount,
		"select count(*) from row")
	if err != nil {
		panic(err)
	}
	return rowCount == 0
}

func getTables(auditDb *sqlx.DB) (tableNames []string) {
	query :=
	`select name from sqlite_master where type='table'`
	//`select name from sqlite_master where type='table' and name like 'G%'`
	rows, err := auditDb.Query(query)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var name sql.NullString
		err = rows.Scan(&name)
		if err != nil {
			panic(err)
		}
		tableNames = append(tableNames, name.String)
	}

	return
}

func tableStart(auditDb *sqlx.DB, tableName string, rowHashes map[string]bool,
meta *Meta) {
	params := map[string]interface{}{"tableName": tableName}
	rows, err := auditDb.NamedQuery(
		"select hash from row where tableName = :tableName",
		params)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var rowHash string
		err = rows.Scan(&rowHash)
		if err != nil {
			panic(err)
		}
		rowHashes[rowHash] = true
	}
}

func processRow(auditDb *sqlx.DB, tableName string,
rowData map[string]interface{},
rowHashes map[string]bool, meta *Meta) (row HashRow, changed bool) {

	meta.rowsProcessed += 1

	row = HashRow{}
	row.Dump, _ = json.Marshal(rowData)
	row.Hash = fmt.Sprintf("%x", md5.Sum([]byte(row.Dump)))

	if rowHashes[row.Hash] {
		delete(rowHashes, row.Hash)
		return row, false
	}

	meta.databaseChanges += 1
	meta.tableChanges += 1
	return row, true
}

func tableFinished(auditDb *sqlx.DB, tableName string, rowBatch []HashRow,
meta *Meta) {
	// TODO Bulk insert?
	// https://github.com/jmoiron/sqlx/issues/134
	insertRow := `insert into row
		(tableName, hash, dump, modified)
		values (:tableName, :hash, :dump, :modified)`
	tx := auditDb.MustBegin()

	for _, row := range rowBatch {
		r := AuditRow{
			TableName: tableName,
			Hash:   row.Hash,
			Dump: getNull(),
			// Seconds end at 19th character, ignore the rest
			Modified:  getTimeStamp(),
		}

		if !firstRun {
			//fmt.Println(string(row.Dump))
			r.Dump = getNullString(string(row.Dump))
		}

		_, err := tx.NamedExec(insertRow, r)
		if err != nil {
			panic(err)
		}
	}

	tx.Commit()
}

func finished(meta *Meta) {
	fmt.Println(fmt.Sprintf("Database changes %d", meta.databaseChanges))
	fmt.Println(fmt.Sprintf("Rows processed %d", meta.rowsProcessed))
	fmt.Println(fmt.Sprintf("Execution time %s", meta.executionTime))
}

func mapTableRows(auditDb *sqlx.DB, targetDb *sqlx.DB, meta *Meta) {
	tableNames := getTables(targetDb)

	for _, tableName := range tableNames {
		rowHashes := make(map[string]bool)
		meta.tableChanges = 0
		tableStart(auditDb, tableName, rowHashes, meta)

		var tableRowCount int
		query := fmt.Sprintf("select count(*) from %s", tableName)
		err := targetDb.Get(&tableRowCount, query)
		if err != nil {
			panic(err)
		}
		rowBatch := make([]HashRow, 0, tableRowCount)

		query = fmt.Sprintf("select * from %s", tableName)
		rows, err := targetDb.Queryx(query)
		if err != nil {
			panic(err)
		}

		for rows.Next() {
			rowData := make(map[string]interface{})
			err = MapScan(rows, rowData)
			if err != nil {
				panic(err)
			}
			row, changed := processRow(
				auditDb, tableName, rowData, rowHashes, meta)
			if changed {
				rowBatch = append(rowBatch, row)
			}
		}

		tableFinished(auditDb, tableName, rowBatch, meta)
		rowBatch = []HashRow{}
		fmt.Println(fmt.Sprintf("%s %d", tableName, meta.tableChanges))
	}
}

func main() {
	start := time.Now()
	meta := Meta{}

	var auditDb *sqlx.DB
	var targetDb *sqlx.DB
	var err error

	auditDb, err = sqlx.Open("sqlite3", "./audit.db")
	defer auditDb.Close()
	if err != nil {
		panic(err)
	}

	targetDb, err = sqlx.Connect("sqlite3", "./Chinook_Sqlite.sqlite")
	defer targetDb.Close()
	if err != nil {
		panic(err)
	}

	firstRun = initAudit(auditDb)

	mapTableRows(auditDb, targetDb, &meta)

	meta.executionTime = time.Since(start)

	finished(&meta)
}
