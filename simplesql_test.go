package simplesql

import (
	"context"
	//	"database/sql"

	//"fmt"
	"log"
	//"strconv"
	"testing"

	_ "github.com/lib/pq"
)

type UserBean struct {
	Username  string `skey:"1"`
	Pwd       string
	Nick_name string
	Real_name string
	Role_id   int
}
type RoleBean struct {
	Role_id   int `skey:"auto"`
	Role_Name string
	Auths     string
	Remark    string
}

func Test_testf(t *testing.T) {
	main(t)

}

func main(t *testing.T) {
	ctx := context.Background()
	ssql, err := New("postgres", "postgres://cxx:123456@localhost/testdb?sslmode=disable")
	if err != nil {
		t.Error(err)
		return
	}

	ssql.RegistTable(&UserBean{}, "wuser")
	ssql.RegistTable(&RoleBean{}, "wrole")

	r, err := ssql.Select(ctx, "wuser", "cxx")
	if err != nil {
		log.Println(err)
		return
	}
	var usr *UserBean = nil
	if len(r) > 0 {
		usr = r[0].(*UserBean)
		log.Println(usr.Pwd)
	}

	res, err := ssql.SelectSingleInt(ctx, "select count(0) from wuser")
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(res)

	resf, err := ssql.SelectSingleFloat(context.Background(), "select count(0) from wuser")
	if err != nil {
		log.Println(err)
	}
	log.Println(resf)

	ress, err := ssql.SelectSingleString(context.Background(), "select count(0) from \"wuser\"")
	if err != nil {
		log.Println(err)
	}
	log.Println(ress)

	rest, err := ssql.SelectSingleTime(context.Background(), "select min(pid) from wauth")
	if err != nil {
		log.Println(err)
	}
	log.Println(rest)

	results := make([]interface{}, 2)
	results[0] = new(int)
	results[1] = new(int)
	err = ssql.Execute(context.Background(), "select * from  P_112(2);", results)
	if err != nil {
		log.Println(err)
	}
	log.Println(*results[1].(*int))

	rss12, err := ssql.Execute2Table(context.Background(), "select * from P_111($1);", "\"User\"", 2)
	if err != nil {
		log.Println(err)
	}
	log.Println(len(rss12))
}

func testftx() {
	sqlh, err := New("postgres", "postgres://cxx:123456@localhost/testdb?sslmode=disable")
	if err != nil {
		log.Println(err)
		return
	}

	sqlh.RegistTable(&UserBean{}, "wuser")
	tx, err := sqlh.Tx(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	ub1 := &UserBean{}
	ub1.Username = "yj3"
	ub1.Real_name = "yyyy"
	tx.Insert(ub1)
	//tx.Insert(&User{})
	defer tx.End()
	tx.CommitForceLater()
}
