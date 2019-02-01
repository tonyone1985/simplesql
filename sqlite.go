// sqlite
package simplesql

import (
	"container/list"
	"context"
	"database/sql"
)

const NTP_NORMAL = 0
const NTP_TXBEGIN = 1
const NTP_TXEND = 2
const NTP_METHODINTX = 3

type Notify struct {
	chfn func()
	chcb chan int
	ntp  int
}

type _sqlite struct {
	_sqlbase
	chManager chan *Notify
}

func (this *_sqlite) StartWork() {
	this.chManager = make(chan *Notify, 10)

	go func() {
		intx := false
		taskCache := list.New()
		for {
			msg := <-this.chManager
			if intx && msg.ntp != NTP_METHODINTX && msg.ntp != NTP_TXEND {
				taskCache.PushBack(msg)
				continue
			}

			if msg.chfn != nil {
				msg.chfn()
			}
			msg.chcb <- 1

			if msg.ntp == NTP_TXBEGIN {
				intx = true
				continue
			}

			if intx && msg.ntp == NTP_TXEND {
				intx = false
				for item := taskCache.Front(); item != nil; item = taskCache.Front() {
					taskCache.Remove(item)
					n := item.Value.(*Notify)
					if n.chfn != nil {
						n.chfn()
					}
					n.chcb <- 1

					if n.ntp == NTP_TXBEGIN {
						intx = true
						break
					}

				}
			}

		}
	}()
}

func (this *_sqlite) callSingle(fn func(), ntp int) {
	downrec := make(chan int)
	this.chManager <- &Notify{
		chfn: fn,
		chcb: downrec,
		ntp:  ntp,
	}
	<-downrec
}

func (this *_sqlite) TxBegin() {
	this.callSingle(nil, NTP_TXBEGIN)
}
func (this *_sqlite) TxEnd() {
	this.callSingle(nil, NTP_TXEND)
}

func (this *_sqlite) _Execute(ctx context.Context, tx *sql.Tx, db *sql.DB, sqlstr string, results []interface{}, paras ...interface{}) error {
	var e error = nil
	ntp := NTP_NORMAL
	if tx != nil {
		ntp = NTP_METHODINTX
	}
	this.callSingle(func() {
		e = this._sqlbase._Execute(ctx, tx, db, sqlstr, results, paras...)
	}, ntp)
	return e
}
func (this *_sqlite) _Execute2Table(ctx context.Context, tx *sql.Tx, db *sql.DB, sqlstr string, table string, paras ...interface{}) ([]interface{}, error) {
	var e error = nil
	var r []interface{} = nil
	ntp := NTP_NORMAL
	if tx != nil {
		ntp = NTP_METHODINTX
	}
	this.callSingle(func() {
		r, e = this._sqlbase._Execute2Table(ctx, tx, db, sqlstr, table, paras...)
	}, ntp)
	return r, e
}
func (this *_sqlite) _DeleteAll(ctx context.Context, tx *sql.Tx, db *sql.DB, tableName string) error {
	var e error = nil
	ntp := NTP_NORMAL
	if tx != nil {
		ntp = NTP_METHODINTX
	}
	this.callSingle(func() {
		e = this._sqlbase._DeleteAll(ctx, tx, db, tableName)
	}, ntp)
	return e
}
func (this *_sqlite) _Insert(ctx context.Context, tx *sql.Tx, db *sql.DB, items ...interface{}) error {
	var e error = nil
	ntp := NTP_NORMAL
	if tx != nil {
		ntp = NTP_METHODINTX
	}
	this.callSingle(func() {
		e = this._sqlbase._Insert(ctx, tx, db, items...)
	}, ntp)
	return e
}
func (this *_sqlite) _Select(ctx context.Context, tx *sql.Tx, db *sql.DB, table string, wherestr string, keys ...interface{}) ([]interface{}, error) {
	var e error = nil
	var r []interface{} = nil
	ntp := NTP_NORMAL
	if tx != nil {
		ntp = NTP_METHODINTX
	}
	this.callSingle(func() {
		r, e = this._sqlbase._Select(ctx, tx, db, table, wherestr, keys...)
	}, ntp)
	return r, e
}
func (this *_sqlite) _Update(ctx context.Context, tx *sql.Tx, db *sql.DB, items ...interface{}) error {
	var e error = nil
	ntp := NTP_NORMAL
	if tx != nil {
		ntp = NTP_METHODINTX
	}
	this.callSingle(func() {
		e = this._sqlbase._Update(ctx, tx, db, items...)
	}, ntp)
	return e
}
