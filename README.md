# gaudit

Golang tool for auditing databases


## Quickstart

Install SQLite sample database 
[Chinook_Sqlite.sqlite](https://chinookdatabase.codeplex.com/) 
in same folder as `audit.go`
    
List tables in default target database
 
    go run audit.go -l

Run audit to get initial database state

    go run audit.go -a
    
Change something in the database and run audit again 
    
View results in `audit.db`

    select * from audit
    order by Modified desc;
    
    
## Reset audit

Changes are audited relative to a snapshot.
The snapshot is created the first time the audit script is executed.
To reset the snapshot run

    drop table audit
    drop table history
    
Doing above is preferable to just removing `audit.db`,
doing this might confuse open sqlite clients


## Using a config file to override the defaults
    
The target database can be set to an ODBC data source.

Create a file `config.json` at the same path as `audit.go`, for example

    {
        "target": {
            "type": "mysql",
            "connectionString": "..."
        }
    }


## Specifying table names and primary keys

By default all the target database tables are audited.
To override this use the config file

    {
        "target": {
            "tables": [
                {
                    "name": "Genre",
                }
            ]
        }
    }


Without primary keys, reverting a row to a state since the last snapshot will 
not be picked up by the audit. To specify primary keys add it to the list of 
tables in the config file

    "tables": [
        {
            "name": "Genre",
            "primaryKey": ["GenreId"]
        }
    ]





