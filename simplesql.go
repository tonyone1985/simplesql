package simplesql

import (
	"context"
	"database/sql"
	"time"
)

const KEYTAG = "key"
const AUTOKEYVAL = "auto"
const DEFKEYVAL = "def"
const FIELDTAG = "col"

type Sql interface {
	//for regist a datatable to relate a go struct
	RegistTable(egptr interface{}, tableName string) error
	//begin a transaction, must call Tx.End() to commit or rollback
	Tx(ctx context.Context) (Tx, error)
	//insert structs to db
	Insert(ctx context.Context, items ...interface{}) error
	//update structs
	Update(ctx context.Context, items ...interface{}) error
	DeleteAll(ctx context.Context, tableName string) error
	Select(ctx context.Context, table string, keys ...interface{}) ([]interface{}, error)
	SelectOne(ctx context.Context, table string, keys ...interface{}) (interface{}, error)
	SelectSingleInt(ctx context.Context, sqlstr string, paras ...interface{}) (int, error)
	SelectSingleFloat(ctx context.Context, sqlstr string, paras ...interface{}) (float32, error)
	SelectSingleString(ctx context.Context, sqlstr string, paras ...interface{}) (string, error)
	SelectSingleTime(ctx context.Context, sqlstr string, paras ...interface{}) (time.Time, error)

	//execute sql string and return 1 row
	Execute(ctx context.Context, sqlstr string, results []interface{}, paras ...interface{}) error
	//execute sql string and return rows
	Execute2Table(ctx context.Context, sqlstr string, table string, paras ...interface{}) ([]interface{}, error)
}

type Tx interface {
	Insert(items ...interface{}) error
	Update(items ...interface{}) error
	DeleteAll(tableName string) error
	Select(table string, keys ...interface{}) ([]interface{}, error)
	SelectOne(table string, keys ...interface{}) (interface{}, error)
	SelectSingleInt(sqlstr string, paras ...interface{}) (int, error)
	SelectSingleFloat(sqlstr string, paras ...interface{}) (float32, error)
	SelectSingleString(sqlstr string, paras ...interface{}) (string, error)
	SelectSingleTime(sqlstr string, paras ...interface{}) (time.Time, error)
	Execute(sqlstr string, results []interface{}, paras ...interface{}) error
	Execute2Table(sqlstr string, table string, paras ...interface{}) ([]interface{}, error)
	CommitForceLater()
	RollbackForceLater()
	End()
}

//create simplesql
func New(driverName string, connstr string) (Sql, error) {
	db, err := sql.Open(driverName, connstr)
	if err != nil {
		return nil, err
	}

	s := &_sql{typeNameMap: make(map[string]*itemControl), tableNameMap: make(map[string]*itemControl), db: db}

	s.driver(driverName)
	return s, nil
}
