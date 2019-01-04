package simplesql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"time"
)

type _sql struct {
	dr           Driver
	tableNameMap map[string]*itemControl
	typeNameMap  map[string]*itemControl
	db           *sql.DB
}

func (this *_sql) Tx(ctx context.Context) (Tx, error) {
	tx, er := this.db.Begin()
	if er != nil {
		return nil, er
	}
	return &_tx{
		dbtx: tx,
		sql:  this,
		ctx:  ctx,
	}, nil
}

func (this *_sql) Execute(ctx context.Context, sqlstr string, results []interface{}, paras ...interface{}) error {
	return this._Execute(ctx, nil, this.db, sqlstr, results, paras...)
}

func (this *_sql) _Execute(ctx context.Context, tx *sql.Tx, db *sql.DB, sqlstr string, results []interface{}, paras ...interface{}) error {

	var e error = nil
	var rs *sql.Rows = nil
	if tx != nil {
		rs, e = tx.QueryContext(ctx, sqlstr, paras...)
	} else if db != nil {
		rs, e = db.QueryContext(ctx, sqlstr, paras...)
	}
	if e != nil {
		return e
	}
	defer rs.Close()
	for rs.Next() {
		if results != nil && len(results) > 0 {
			rs.Scan(results...)
		}
		return nil
	}
	return errors.New("no result")
}
func (this *_sql) Execute2Table(ctx context.Context, sqlstr string, table string, paras ...interface{}) ([]interface{}, error) {
	return this._Execute2Table(ctx, nil, this.db, sqlstr, table, paras...)
}
func (this *_sql) _Execute2Table(ctx context.Context, tx *sql.Tx, db *sql.DB, sqlstr string, table string, paras ...interface{}) ([]interface{}, error) {
	ctl := this.tableNameMap[table]
	if ctl == nil {
		return nil, errors.New("table not registed " + table)
	}

	var e error = nil
	var rs *sql.Rows = nil
	if tx != nil {
		rs, e = tx.QueryContext(ctx, sqlstr, paras...)
	} else if db != nil {
		rs, e = db.QueryContext(ctx, sqlstr, paras...)
	}
	if e != nil {
		return nil, e
	}
	defer rs.Close()

	return ctl.scan(rs)

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
func (this *_sql) DeleteAll(ctx context.Context, tableName string) error {
	return this._DeleteAll(ctx, nil, this.db, tableName)
}
func (this *_sql) _DeleteAll(ctx context.Context, tx *sql.Tx, db *sql.DB, tableName string) error {
	var e error = nil
	if tx != nil {
		_, e = tx.ExecContext(ctx, "delete from "+this.dr.FieldFlag1()+tableName+this.dr.FieldFlag2()+";")
	} else if db != nil {
		_, e = db.ExecContext(ctx, "delete from "+this.dr.FieldFlag1()+tableName+this.dr.FieldFlag2()+";")
	}
	return e
}
func (this *_sql) getCtlByTableName(tableName string) (*itemControl, error) {
	fm := this.tableNameMap[tableName]
	if fm == nil {
		return nil, errors.New("table not registed")
	}
	return fm, nil
}
func (this *_sql) getCtlByItem(item interface{}) (*itemControl, error) {
	ot := reflect.TypeOf(item)
	if ot.Kind() == reflect.Ptr {
		ot = ot.Elem()
	}

	fm := this.typeNameMap[ot.String()]

	if fm == nil {
		return nil, errors.New("table not registed")
	}
	return fm, nil
}

func (this *_sql) RegistTable(eg interface{}, tableName string) error {
	ptype := reflect.TypeOf(eg)
	vtype := ptype.Elem()
	keyfields := make([]*fieldItem, 0)
	normalfields := make([]*fieldItem, 0)
	for i, fl := 0, vtype.NumField(); i < fl; i++ {
		if vtype.Field(i).Tag.Get(STRUCTTAG) == "" {
			normalfields = append(normalfields, &fieldItem{FIELD_TYPE_NORMAL, i, vtype.Field(i).Name})
		} else if vtype.Field(i).Tag.Get(STRUCTTAG) == AUTOKEYVAL {
			keyfields = append(keyfields, &fieldItem{FIELD_TYPE_AUTOKEY, i, vtype.Field(i).Name})
		} else {
			keyfields = append(keyfields, &fieldItem{FIELD_TYPE_DEFKEY, i, vtype.Field(i).Name})
		}
	}

	te := &itemControl{vtype, ptype, tableName, vtype.NumField(), this, keyfields, normalfields,
		len(keyfields), len(normalfields),
	}
	this.typeNameMap[vtype.String()] = te
	this.tableNameMap[tableName] = te
	return nil
}

func (this *_sql) doUpdate(ctx context.Context, ictl *itemControl, tx *sql.Tx, db *sql.DB, items ...interface{}) error {
	stmt, err := ictl.SqlUpdateStmt(ctx, tx, db)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, v := range items {
		_, err := stmt.ExecContext(ctx, ictl.GetFieldValuesUpdate(v)...)
		if err != nil {
			return err
		}
	}
	return err
}

func (this *_sql) doInsert(ctx context.Context, ictl *itemControl, tx *sql.Tx, db *sql.DB, items ...interface{}) error {

	stmt, err := ictl.SQLInsertStmt(ctx, tx, db)

	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, v := range items {
		_, err := stmt.ExecContext(ctx, ictl.GetFieldValuesInsert(v)...)
		if err != nil {
			return err
		}
	}

	return err
}

type itemGroup struct {
	ctl   *itemControl
	items []interface{}
}

func (this *_sql) getGroups(items ...interface{}) ([]*itemGroup, error) {
	cais := make([]*itemGroup, 0)
	for _, v := range items {
		haveSame := false
		for _, icais := range cais {
			if icais.ctl.IsSame(v) {
				icais.items = append(icais.items, v)
				haveSame = true
				break
			}
		}
		if !haveSame {
			_ctl, er := this.getCtlByItem(v)
			if er != nil {
				return nil, er
			}
			objs := make([]interface{}, 1)
			objs[0] = v
			cais = append(cais, &itemGroup{_ctl, objs})

		}
	}
	return cais, nil
}
func (this *_sql) Insert(ctx context.Context, items ...interface{}) error {
	return this._Insert(ctx, nil, this.db, items...)
}
func (this *_sql) _Insert(ctx context.Context, tx *sql.Tx, db *sql.DB, items ...interface{}) error {
	if len(items) == 0 {
		return errors.New("error arr len")
	}
	cais, err := this.getGroups(items...)
	if err != nil {
		return err
	}

	for _, i := range cais {
		if er := this.doInsert(ctx, i.ctl, tx, db, i.items...); er != nil {
			return er
		}
	}
	return nil
}
func (this *_sql) Select(ctx context.Context, table string, keys ...interface{}) ([]interface{}, error) {
	return this._Select(ctx, nil, this.db, table, keys...)
}
func (this *_sql) _Select(ctx context.Context, tx *sql.Tx, db *sql.DB, table string, keys ...interface{}) ([]interface{}, error) {
	ctl, err := this.getCtlByTableName(table)
	if err != nil {
		return nil, err
	}

	stmt, paras, err := ctl.SqlSelectStmt(ctx, tx, db, keys...)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	rs, err := stmt.QueryContext(ctx, paras...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	return ctl.scan(rs)

}
func (this *_sql) Update(ctx context.Context, items ...interface{}) error {
	return this._Update(ctx, nil, this.db, items...)
}
func (this *_sql) _Update(ctx context.Context, tx *sql.Tx, db *sql.DB, items ...interface{}) error {

	cais, err := this.getGroups(items...)
	if err != nil {
		return err
	}

	for _, i := range cais {
		if er := this.doUpdate(ctx, i.ctl, tx, db, i.items...); er != nil {
			return er
		}
	}
	return nil

}

func (this *_sql) driver(driverName string) {
	if "postgres" == driverName {
		this.dr = &postgresDriver{}
	} else {
		this.dr = &defaultDriver{}
	}
}

const FIELD_TYPE_AUTOKEY = 1
const FIELD_TYPE_DEFKEY = 2
const FIELD_TYPE_NORMAL = 0

type fieldItem struct {
	ftp  int
	idx  int
	name string
}

type itemControl struct {
	vType           reflect.Type
	pType           reflect.Type
	tableName       string
	filedLen        int
	sql             *_sql
	keyfields       []*fieldItem
	normalfields    []*fieldItem
	keyfieldsLen    int
	normalfieldsLen int
}

func (this *itemControl) IsSame(item interface{}) bool {
	vty := reflect.TypeOf(item)
	if vty.Kind() == reflect.Ptr {
		vty = vty.Elem()
	}
	return vty.String() == this.vType.String()
}

func (this *itemControl) prepareStmt(ctx context.Context, tx *sql.Tx, db *sql.DB, sqlstr string) (*sql.Stmt, error) {
	if tx != nil {
		return tx.PrepareContext(ctx, sqlstr)
	}
	if db != nil {
		return db.PrepareContext(ctx, sqlstr)
	}
	return nil, errors.New("tx & db both nil")
}

func (this *itemControl) Allfields() string {
	fieldstag := ""
	for i := 0; i < this.normalfieldsLen; i++ {
		if fieldstag != "" {
			fieldstag += ","
		}
		fieldstag += this.sql.dr.FieldFlag1() + this.normalfields[i].name + this.sql.dr.FieldFlag2()
	}
	for i := 0; i < this.keyfieldsLen; i++ {
		if fieldstag != "" {
			fieldstag += ","
		}
		fieldstag += this.sql.dr.FieldFlag1() + this.keyfields[i].name + this.sql.dr.FieldFlag2()
	}
	return fieldstag
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func (this *itemControl) whereSelect(args ...interface{}) (string, []interface{}) {
	wheretag := " where "

	pc := this.sql.dr.ParameterContext()

	fieldcnt := min(len(args), this.keyfieldsLen)

	paras := make([]interface{}, 0)

	for i := 0; i < fieldcnt; i++ {
		if args[i] == nil {
			continue
		}
		if len(paras) > 0 {
			wheretag += " and"
		}
		wheretag += " " + this.sql.dr.FieldFlag1() + this.keyfields[i].name + this.sql.dr.FieldFlag2() + "=" + pc.Parameter()
		paras = append(paras, args[i])
	}
	return wheretag, paras
}
func (this *itemControl) getFieldSaveAddrs(obj interface{}) []interface{} {
	arr := make([]interface{}, this.normalfieldsLen+this.keyfieldsLen)
	vt := reflect.ValueOf(obj)
	if vt.Kind() == reflect.Ptr {
		vt = vt.Elem()
	}

	for i := 0; i < this.normalfieldsLen; i++ {
		arr[i] = vt.Field(this.normalfields[i].idx).Addr().Interface()

	}
	for i := 0; i < this.keyfieldsLen; i++ {
		arr[this.normalfieldsLen+i] = vt.Field(this.keyfields[i].idx).Addr().Interface()
	}
	return arr
}
func (this *itemControl) scan(row *sql.Rows) ([]interface{}, error) {
	var objs []interface{}
	for row.Next() {
		obj := reflect.New(this.vType).Interface()

		err := row.Scan(this.getFieldSaveAddrs(obj)...)
		if err != nil {
			return nil, err
		}

		objs = append(objs, obj)
	}
	return objs, nil
}
func (this *itemControl) SqlSelectStmt(ctx context.Context, tx *sql.Tx, db *sql.DB, paras ...interface{}) (*sql.Stmt, []interface{}, error) {

	wheresql, paras := this.whereSelect(paras...)

	sqlstr := "SELECT " + this.Allfields() +
		" FROM " + this.tableName + " " + wheresql
	stmt, err := this.prepareStmt(ctx, tx, db, sqlstr)
	if err != nil {
		return nil, nil, err
	}
	return stmt, paras, nil

}

func (this *itemControl) SqlUpdateStmt(ctx context.Context, tx *sql.Tx, db *sql.DB) (*sql.Stmt, error) {
	var tagsStr string
	pc := this.sql.dr.ParameterContext()

	for i := 0; i < this.normalfieldsLen; i++ {

		if len(tagsStr) > 0 {
			tagsStr += ", "
		}
		tagsStr += this.sql.dr.FieldFlag1()
		tagsStr += this.normalfields[i].name
		tagsStr += this.sql.dr.FieldFlag2()
		tagsStr += " = " + pc.Parameter()
	}
	if len(tagsStr) > 0 {
		tagsStr += " "
		tagsStr = " " + tagsStr
	}

	wherestr := " where " + this.sql.dr.FieldFlag1() + this.keyfields[0].name + this.sql.dr.FieldFlag2() + " = " + pc.Parameter()

	for i := 1; i < this.keyfieldsLen; i++ {
		wherestr += " and " + this.sql.dr.FieldFlag1() + this.keyfields[i].name + this.sql.dr.FieldFlag2() + " = " + pc.Parameter()
	}
	sqlstr := "UPDATE " + this.sql.dr.FieldFlag1() + this.tableName + this.sql.dr.FieldFlag2() + " SET " + tagsStr + wherestr

	return this.prepareStmt(ctx, tx, db, sqlstr)
}

func (this *itemControl) SQLInsertStmt(ctx context.Context, tx *sql.Tx, db *sql.DB) (*sql.Stmt, error) {
	var vs string
	pc := this.sql.dr.ParameterContext()
	for i := 0; i < this.keyfieldsLen; i++ {
		if this.keyfields[i].ftp == FIELD_TYPE_AUTOKEY {
			continue
		}
		if len(vs) > 0 {
			vs += ", "
		}
		vs += pc.Parameter()
	}
	for i := 0; i < this.normalfieldsLen; i++ {
		if len(vs) > 0 {
			vs += ", "
		}
		vs += pc.Parameter()
	}

	var tagsStr string
	for i := 0; i < this.keyfieldsLen; i++ {
		if this.keyfields[i].ftp == FIELD_TYPE_AUTOKEY {
			continue
		}
		if len(tagsStr) > 0 {
			tagsStr += ", "
		}
		tagsStr += this.sql.dr.FieldFlag1()
		tagsStr += this.keyfields[i].name
		tagsStr += this.sql.dr.FieldFlag2()
	}
	for i := 0; i < this.normalfieldsLen; i++ {

		if len(tagsStr) > 0 {
			tagsStr += ", "
		}
		tagsStr += this.sql.dr.FieldFlag1()
		tagsStr += this.normalfields[i].name
		tagsStr += this.sql.dr.FieldFlag2()
	}
	sqlstr := "INSERT INTO " + this.sql.dr.FieldFlag1() + this.tableName + this.sql.dr.FieldFlag2() + " ( " + tagsStr + " ) " +
		"VALUES (" + vs + ")"

	return this.prepareStmt(ctx, tx, db, sqlstr)
}

func (this *itemControl) GetFieldValuesInsert(obj interface{}) []interface{} {
	//elem.Field(i).Addr().Interface()
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	values := make([]interface{}, 0)
	for i := 0; i < this.keyfieldsLen; i++ {
		if this.keyfields[i].ftp == FIELD_TYPE_AUTOKEY {
			continue
		}
		str := val.String()
		num := val.NumField()

		_, _ = num, str
		values = append(values, val.Field(this.keyfields[i].idx).Addr().Interface())
	}
	for i := 0; i < this.normalfieldsLen; i++ {
		values = append(values, val.Field(this.normalfields[i].idx).Addr().Interface())
	}

	return values
}
func (this *itemControl) GetFieldValuesUpdate(obj interface{}) []interface{} {
	//elem.Field(i).Addr().Interface()
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	values := make([]interface{}, this.keyfieldsLen+this.normalfieldsLen)
	pos := 0

	for i := 0; i < this.normalfieldsLen; i++ {
		values[pos] = val.Field(this.normalfields[i].idx).Addr().Interface()
		pos++
	}
	for i := 0; i < this.keyfieldsLen; i++ {
		values[pos] = val.Field(this.keyfields[i].idx).Addr().Interface()
		pos++
	}

	return values
}

type IParameterContext interface {
	Parameter() string
}

type Driver interface {
	ParameterContext() IParameterContext
	FieldFlag1() string
	FieldFlag2() string
}

type DefaultParameterContext struct {
}

func (this *DefaultParameterContext) Parameter() string {
	return "?"
}

type defaultDriver struct {
}

func (this *defaultDriver) FieldFlag1() string {
	return "`"
}
func (this *defaultDriver) FieldFlag2() string {
	return "`"
}
func (this *defaultDriver) ParameterContext() IParameterContext {
	return &DefaultParameterContext{}
}

type PostgresParameterContext struct {
	pidx int64
}

func (this *PostgresParameterContext) Parameter() string {
	this.pidx++

	return "$" + strconv.FormatInt(this.pidx, 10)
}

type postgresDriver struct {
}

func (this *postgresDriver) FieldFlag1() string {
	return ""
	//return "\""
}
func (this *postgresDriver) FieldFlag2() string {
	return ""
	//return "\""
}
func (this *postgresDriver) ParameterContext() IParameterContext {
	return &PostgresParameterContext{}
}