package simplesql

import (
	"context"
	"log"
	"simplesql"

	_ "github.com/lib/pq"
)

var ssql simplesql.Sql

//simple struct
type UserBean struct {
	Username  string //first field of the simple struct will be default key if  has no skey field
	Pwd       string //field name will be the column name if there is no scol tag
	Nick_name string
	Real_name string
	Role_id   int
}

type UserBean2 struct {
	Username  string `key:"auto" col:"username"` //all skey field will not be update when execute update;
	Pwd       string `col:"pwd"`
	Nick_name string `key:"def" col:"nick_name"` //the skey fields will be used by where  when execute update;
	Real_name string `col:"real_name"`
	Role_id   int    `col:"role_id"`
}

type RoleBean struct {
	Role_id    int `key:"auto" col:"role_id"` //auto skey will not be used when execute insert ; tag scol will be the column name
	Role_Name2 string
	Auths      string
	Remark     string
}

func tinit() {
	//simplesql.New("postgresql", "postgres://cxx:123456@localhost/testdb?sslmode=disable")

	var err error
	ssql, err = simplesql.New("postgres", "postgres://postgres:postgres@localhost/dbtest?sslmode=disable")
	if err != nil {
		log.Println(err)
		return
	}

	ssql.RegistTable(&UserBean{}, "wuser")
	ssql.RegistTable(&RoleBean{}, "wrole")
}

func testinsert() {
	tinit()
	ctx := context.Background()
	r1 := &RoleBean{
		Role_Name: "rn",
		Auths:     "1,2,3,4,5",
		Remark:    "rremark",
	}
	r2 := &RoleBean{
		Role_Name: "rn",
		Auths:     "1,2,3,4,5",
		Remark:    "rremark",
	}
	u := &UserBean{
		Username:  "tony",
		Pwd:       "123",
		Nick_name: "nickname",
		Real_name: "realname",
		Role_id:   1,
	}
	er := ssql.Insert(ctx, r1, r2, u)
	if er != nil {
		log.Println(er)
	}
}
func testupdate() {
	tinit()
	ctx := context.Background()
	u := &UserBean{
		Username:  "tony",
		Pwd:       "1333",
		Nick_name: "nick1name",
		Real_name: "real2name",
		Role_id:   3,
	}
	er := ssql.Update(ctx, u)
	if er != nil {
		log.Println(er)
	}
}

func testselect() {
	tinit()
	ctx := context.Background()

	r, er := ssql.SelectOne(ctx, "wuser", "tony")
	if er != nil {
		log.Println(er)
		return
	}
	if r == nil {
		log.Println("no result")
		return
	}
	log.Println(r.(*UserBean).Real_name)
}

var ssql Sql

func sqlitetestsmp() {

	var err error
	ctx := context.Background()
	//ctx2 := context.Background()

	//ssql, err = simplesql.New("mysql", "root:123456@tcp(127.0.0.1:3306)/test")
	ssql, err = simplesql.New("sqlite3", "./foo.db")
	checkErr(err)

	downtelme := make(chan int)
	for j := 0; j < 100; j++ {
		go func() {
			for i := 0; i < CNT+1; i++ {
				sqlinserts(ctx)
				log.Println("insert", i)
			}
			//downtelme <- 1
		}()
		go func() {
			for i := 0; i < CNT; i++ {
				sqldels(ctx)
				log.Println("del", i)
			}
		}()
	}
	for i := 0; i < CNT; i++ {
		sqldels(ctx)
		log.Println("del", i)
	}

	<-downtelme

}
func sqlinserts(ctx context.Context) {

	err := ssql.Execute(ctx, "INSERT INTO userinfo(username, departname, created) values(?,?,?)", nil, "astaxie", "研发部门44444", "2012-12-09")

	checkErr(err)

}
func sqldels(ctx context.Context) {
	//stmt, err := db.Prepare("select uid from userinfo LIMIT 1")
	tx, _ := ssql.Tx(ctx)
	defer tx.End()
	uid, err := tx.SelectSingleInt("select uid from userinfo LIMIT 1")

	checkErr(err)
	_ = uid
	err = tx.Execute("delete from userinfo where uid=?", nil, uid)
	checkErr(err)

}
