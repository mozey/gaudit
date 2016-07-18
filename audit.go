package main

import (
	"./utils"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"time"
)

var firstRun bool
var verbose bool = false

type Meta struct {
	rowsProcessed   int
	databaseChanges int
	tableChanges    map[string]int
	executionTime   time.Duration
}

// Audit database

type AuditRow struct {
	TableName  string         `db:"TableName"`
	PrimaryKey string         `db:"RowHash"`
	RowHash    string         `db:"RowHash"`
	RowDump    sql.NullString `db:"RowDump"`
	Modified   string         `db:"Modified"`
}

type HistoryRow struct {
	ExecutionTimestamp string `db:"ExecutionTimestamp"`
	Key                string `db:"Key"`
	Value              string `db:"Value"`
}

type RowMap map[string]AuditRow

// Config

type AuditConfig struct {
	Type             string `json:"type"`
	ConnectionString string `json:"connectionString"`
}
type TargetConfig struct {
	Type             string `json:"type"`
	ConnectionString string `json:"connectionString"`
	Tables           []struct {
		Name       string   `json:"name"`
		PrimaryKey []string `json:"primaryKey"`
	} `json:"tables"`
}
type Config struct {
	Audit  AuditConfig  `json:"audit"`
	Target TargetConfig `json:"target"`
}

// config.json must exist at the same path as audit
func (c *Config) load() {
	file, err := os.Open("./config.json")
	if err == nil {
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&config)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// Set default values for config
var config = Config{
	Audit: AuditConfig{
		Type:             "sqlite3",
		ConnectionString: "./audit.db",
	},
	Target: TargetConfig{
		Type:             "sqlite3",
		ConnectionString: "./Chinook_Sqlite.sqlite",
	},
}

// Connections
type Connections struct {
	Audit  *sqlx.DB
	Target *sqlx.DB
}

func (c *Connections) connect() {
	var err error

	c.Audit, err = sqlx.Open(config.Audit.Type, config.Audit.ConnectionString)
	if err != nil {
		log.Fatal(err)
	}

	c.Target, err = sqlx.Connect(
		config.Target.Type, config.Target.ConnectionString)
	if err != nil {
		log.Fatal(err)
	}
}
func (c *Connections) close() {
	c.Audit.Close()
	c.Target.Close()
}

var conns = Connections{}

// initAudit initialises audit.db the first time `gaudit -a` is executed.
// To reset run "delete from audit" or delete audit.db.
func initAudit() (firstRun bool) {
	// Create audit.db tables if not exists
	schema := `
	create table if not exists
		audit (TableName, PrimaryKey, RowHash, RowDump, Modified);
	create table if not exists
		history (ExecutionTimestamp, Key, Value);
	`
	conns.Audit.Exec(schema)

	// Return true if the audit table is empty
	var auditCount int
	err := conns.Audit.Get(&auditCount,
		"select count(*) from audit")
	if err != nil {
		log.Fatal(err)
	}
	return auditCount == 0
}

func getTables() (tableNames []string) {
	if config.Target.Tables != nil {
		for _, v := range config.Target.Tables {
			tableNames = append(tableNames, v.Name)
		}

	} else {
		query :=
			`select name from sqlite_master where type='table'`
		rows, err := conns.Target.Query(query)
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var name sql.NullString
			err = rows.Scan(&name)
			if err != nil {
				log.Fatal(err)
			}
			tableNames = append(tableNames, name.String)
		}
	}

	return tableNames
}

func tableStart(tableName string, rowHashes map[string]bool,
	meta *Meta) {
	params := map[string]interface{}{"tableName": tableName}
	rows, err := conns.Audit.NamedQuery(
		"select RowHash from audit where tableName = :tableName",
		params)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var rowHash string
		err = rows.Scan(&rowHash)
		if err != nil {
			log.Fatal(err)
		}
		rowHashes[rowHash] = true
	}
}

func processRow(tableName string,
	rowData map[string]interface{},
	rowHashes map[string]bool,
	meta *Meta) (ar AuditRow, changed bool) {

	meta.rowsProcessed += 1

	rowDump, _ := json.Marshal(rowData)
	ar.RowDump = sql.NullString{String: string(rowDump), Valid: true}
	ar.RowHash = fmt.Sprintf("%x", md5.Sum(rowDump))

	if rowHashes[ar.RowHash] {
		delete(rowHashes, ar.RowHash)
		return ar, false
	}

	meta.databaseChanges += 1
	meta.tableChanges[tableName] += 1
	return ar, true
}

func tableFinished(tableName string, batch []AuditRow, meta *Meta) {
	// TODO Bulk insert?
	// https://github.com/jmoiron/sqlx/issues/134
	insertRow := `insert into audit
		(TableName, RowHash, RowDump, Modified)
		values (:TableName, :RowHash, :RowDump, :Modified)`
	tx := conns.Audit.MustBegin()

	for _, ar := range batch {
		ar.TableName = tableName
		// Seconds end at 19th character, ignore the rest
		ar.Modified = fmt.Sprintf("%.19s", time.Now().UTC())

		if firstRun {
			ar.RowDump = sql.NullString{String: "", Valid: false}
		}

		_, err := tx.NamedExec(insertRow, ar)
		if err != nil {
			log.Fatal(err)
		}
	}

	tx.Commit()
}

func mapTableRows(meta *Meta) {
	tableNames := getTables()

	for _, tableName := range tableNames {
		rowHashes := make(map[string]bool)
		if meta.tableChanges == nil {
			meta.tableChanges = make(map[string]int)
		}
		meta.tableChanges[tableName] = 0
		tableStart(tableName, rowHashes, meta)

		var tableRowCount int
		query := fmt.Sprintf("select count(*) from %s", tableName)
		err := conns.Target.Get(&tableRowCount, query)
		if err != nil {
			log.Fatal(err)
		}
		batch := make([]AuditRow, 0, tableRowCount)

		query = fmt.Sprintf("select * from %s", tableName)
		rows, err := conns.Target.Queryx(query)
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			rowData := make(map[string]interface{})
			err = utils.MapScan(rows, rowData)
			if err != nil {
				log.Fatal(err)
			}
			row, changed := processRow(tableName, rowData, rowHashes, meta)
			if changed {
				batch = append(batch, row)
			}
		}

		tableFinished(tableName, batch, meta)
		if verbose {
			log.Println(fmt.Sprintf(
				"%s %d", tableName, meta.tableChanges[tableName]))
		}
	}
}

func insertHistoryRow(ExecutionTimestamp string, key string, value string) {
	hr := HistoryRow{
		ExecutionTimestamp: ExecutionTimestamp,
		Key:                key,
		Value:              value,
	}
	insertRow := `insert into history
		(ExecutionTimestamp, Key, Value)
		values (:ExecutionTimestamp, :Key, :Value)`
	_, err := conns.Audit.NamedExec(insertRow, hr)
	if err != nil {
		log.Fatal(err)
	}
}

func finished(meta *Meta, history int) {
	databaseChanges := "Database changes"
	rowsProcessed := "Rows processed"
	executionTime := "Execution time"

	// TODO Clear history for old audit runs

	// Save history for this audit run
	if history > 0 {
		// Seconds end at 19th character, ignore the rest
		ExecutionTimestamp := fmt.Sprintf("%.19s", time.Now().UTC())
		for tableName, v := range meta.tableChanges {
			insertHistoryRow(
				ExecutionTimestamp,
				fmt.Sprintf("Table %s", tableName),
				fmt.Sprintf("%d", v))
		}
		insertHistoryRow(ExecutionTimestamp,
			databaseChanges, fmt.Sprintf("%d", meta.databaseChanges))
		insertHistoryRow(ExecutionTimestamp,
			rowsProcessed, fmt.Sprintf("%d", meta.rowsProcessed))
		insertHistoryRow(ExecutionTimestamp,
			executionTime, fmt.Sprintf("%s", meta.executionTime))
	}

	// Print results
	if verbose {
		log.Println(fmt.Sprintf("%s %d", databaseChanges, meta.databaseChanges))
		log.Println(fmt.Sprintf("%s %d", rowsProcessed, meta.rowsProcessed))
		log.Println(fmt.Sprintf("%s %s", executionTime, meta.executionTime))
	}
}

func init() {
	// https://golang.org/pkg/log/#pkg-constants
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	listTables := flag.Bool("l", false, "List tables")
	runAudit := flag.Bool("a", false, "Audit")
	printConfig := flag.Bool("c", false, "Print config")
	history := flag.Int("h", 0, "Save history")
	flag.BoolVar(&verbose, "v", false, "Print results of audit run")
	flag.Parse()

	config.load()

	if *listTables {
		conns.connect()
		defer conns.close()
		fmt.Println(getTables())

	} else if *runAudit {
		start := time.Now()
		conns.connect()
		defer conns.close()
		meta := Meta{}
		firstRun = initAudit()
		mapTableRows(&meta)
		meta.executionTime = time.Since(start)
		finished(&meta, *history)

	} else if *printConfig {
		fmt.Println(utils.JsonDump(config, true))

	} else {
		flag.Usage()
	}
}
