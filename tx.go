package simplesql

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type _tx struct {
	sql  *_sql
	ctx  context.Context
	dbtx *sql.Tx
	e    error
}

func (this *_tx) CommitForceLater() {
	this.e = nil
}
func (this *_tx) RollbackForceLater() {
	this.e = errors.New("RollbackForce")
}

func (this *_tx) End() {
	if this.e != nil {
		this.dbtx.Rollback()
	} else {
		this.dbtx.Commit()
	}
}
func (this *_tx) Insert(items ...interface{}) error {
	this.e = this.sql._Insert(this.ctx, this.dbtx, nil, items...)
	return this.e
}
func (this *_tx) Update(items ...interface{}) error {
	this.e = this.sql._Update(this.ctx, this.dbtx, nil, items...)
	return this.e
}
func (this *_tx) DeleteAll(tableName string) error {
	this.e = this.sql._DeleteAll(this.ctx, this.dbtx, nil, tableName)
	return this.e
}
func (this *_tx) Select(table string, keys ...interface{}) ([]interface{}, error) {
	r, e := this.sql._Select(this.ctx, this.dbtx, nil, table, keys...)
	this.e = e
	return r, e
}
func (this *_tx) SelectSingleInt(sqlstr string, paras ...interface{}) (int, error) {
	var r int
	this.e = this.sql.selectSingle(this.ctx, this.dbtx, nil, sqlstr, &r, paras...)

	return r, this.e
}
func (this *_tx) SelectSingleFloat(sqlstr string, paras ...interface{}) (float32, error) {
	var r float32
	this.e = this.sql.selectSingle(this.ctx, this.dbtx, nil, sqlstr, &r, paras...)
	return r, this.e
}
func (this *_tx) SelectSingleString(sqlstr string, paras ...interface{}) (string, error) {
	var r string
	this.e = this.sql.selectSingle(this.ctx, this.dbtx, nil, sqlstr, &r, paras...)
	return r, this.e
}
func (this *_tx) SelectSingleTime(sqlstr string, paras ...interface{}) (time.Time, error) {
	var r time.Time
	this.e = this.sql.selectSingle(this.ctx, this.dbtx, nil, sqlstr, &r, paras...)
	return r, this.e
}
func (this *_tx) Execute(sqlstr string, results []interface{}, paras ...interface{}) error {
	this.e = this.sql.selectSingle(this.ctx, this.dbtx, nil, sqlstr, results, paras...)
	return this.e
}
func (this *_tx) Execute2Table(sqlstr string, table string, paras ...interface{}) ([]interface{}, error) {
	r, e := this.sql._Execute2Table(this.ctx, this.dbtx, nil, sqlstr, table, paras...)
	this.e = e
	return r, e
}
