package simplesql

import (
	"context"
	"database/sql"

	"time"
)

type _sql struct {
	Sqlbase
	db *sql.DB
}

func (this *_sql) Tx(ctx context.Context) (Tx, error) {
	this.TxBegin()
	tx, er := this.db.Begin()
	if er != nil {
		return nil, er
		this.TxEnd()
	}

	return &_tx{
		dbtx: tx,
		sql:  this,
		ctx:  ctx,
	}, nil
}
func (this *_sql) SelectOne(ctx context.Context, table string, keys ...interface{}) (interface{}, error) {
	r, e := this.Select(ctx, table, keys...)
	if e != nil {
		return nil, e
	}
	if len(r) == 0 {
		return nil, nil
	}
	return r[0], nil
}

func (this *_sql) Execute(ctx context.Context, sqlstr string, results []interface{}, paras ...interface{}) error {
	return this._Execute(ctx, nil, this.db, sqlstr, results, paras...)
}

func (this *_sql) Execute2Table(ctx context.Context, sqlstr string, table string, paras ...interface{}) ([]interface{}, error) {
	return this._Execute2Table(ctx, nil, this.db, sqlstr, table, paras...)
}
func (this *_sql) Execute2Interfaces(ctx context.Context, sqlstr string, paras ...interface{}) ([]interface{}, error) {
	return this._Execute2Interfaces(ctx, nil, this.db, sqlstr, paras...)
}

func (this *_sql) SelectSingleInt(ctx context.Context, sqlstr string, paras ...interface{}) (int, error) {
	var r int
	return r, this.selectSingle(ctx, nil, this.db, sqlstr, &r, paras...)
}
func (this *_sql) SelectSingleFloat(ctx context.Context, sqlstr string, paras ...interface{}) (float32, error) {
	var r float32
	return r, this.selectSingle(ctx, nil, this.db, sqlstr, &r, paras...)
}
func (this *_sql) SelectSingleString(ctx context.Context, sqlstr string, paras ...interface{}) (string, error) {
	var r string
	return r, this.selectSingle(ctx, nil, this.db, sqlstr, &r, paras...)
}
func (this *_sql) SelectSingleTime(ctx context.Context, sqlstr string, paras ...interface{}) (time.Time, error) {
	var r time.Time
	return r, this.selectSingle(ctx, nil, this.db, sqlstr, &r, paras...)
}
func (this *_sql) selectSingle(ctx context.Context, tx *sql.Tx, db *sql.DB, sqlstr string, r interface{}, paras ...interface{}) error {
	rst := make([]interface{}, 1)
	rst[0] = r
	return this._Execute(ctx, tx, db, sqlstr, rst, paras...)

}

func (this *_sql) Delete(ctx context.Context, tableName string, keys ...interface{}) error {
	return this._Delete(ctx, nil, this.db, tableName, keys...)
}

func (this *_sql) DeleteAll(ctx context.Context, tableName string) error {
	return this._DeleteAll(ctx, nil, this.db, tableName)
}
func (this *_sql) Insert(ctx context.Context, items ...interface{}) error {
	return this._Insert(ctx, nil, this.db, items...)
}

func (this *_sql) InsertSafe(ctx context.Context, items ...interface{}) error {
	return this._InsertSafe(ctx, nil, this.db, items...)
}

func (this *_sql) Select(ctx context.Context, table string, keys ...interface{}) ([]interface{}, error) {
	return this._Select(ctx, nil, this.db, table, "", keys...)
}

func (this *_sql) SelectWhere(ctx context.Context, table string, wherestr string, keys ...interface{}) ([]interface{}, error) {
	return this._Select(ctx, nil, this.db, table, wherestr, keys...)
}

func (this *_sql) Update(ctx context.Context, items ...interface{}) error {
	return this._Update(ctx, nil, this.db, items...)
}
