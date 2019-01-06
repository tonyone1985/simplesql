package main

import (
	"context"
	"log"
	"simplesql"
	"strings"

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
	Role_id    int    `key:"auto" col:"role_id"` //auto skey will not be used when execute insert ; tag scol will be the column name
	Role_Name2 string `col:"role_name"`
	Auths      string
	Remark     string
}
type AuthBean struct {
	Auth_id    int `key:"auto"`
	Auth_Name  string
	Auth_Val   string
	Auth_Order int
}

func init() {
	//simplesql.New("postgresql", "postgres://cxx:123456@localhost/testdb?sslmode=disable")

	var err error
	ssql, err = simplesql.New("postgres", "postgres://postgres:postgres@localhost/dbtest?sslmode=disable")
	if err != nil {
		log.Println(err)
		return
	}

	ssql.RegistTable(&UserBean{}, "wuser")
	ssql.RegistTable(&RoleBean{}, "wrole")
	ssql.RegistTable(&AuthBean{}, "wauth")
}

func testinsert() {
	ctx := context.Background()
	r1 := &RoleBean{
		Role_Name2: "rn",
		Auths:      "1,2,3,4,5",
		Remark:     "rremark",
	}
	r2 := &RoleBean{
		Role_Name2: "rn",
		Auths:      "1,2,3,4,5",
		Remark:     "rremark",
	}
	u := &UserBean{
		Username:  "tony2",
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

	rsss, er := ssql.Select(ctx, "wrole", 1, "rn")
	if er != nil {
		log.Println("no result")
		return
	}
	for _, i := range rsss {
		log.Println(i.(*RoleBean).Role_id)
	}

}

func login(ctx context.Context, uname string, pwd string) (*UserBean, *RoleBean, []*AuthBean) {
	u, er := ssql.SelectOne(ctx, "wuser", strings.ToLower(uname))
	if er != nil || u == nil || u.(*UserBean).Pwd != pwd {
		log.Println(er)
		return nil, nil, nil
	}
	uu := u.(*UserBean)
	r, er := ssql.SelectOne(ctx, "wrole", uu.Role_id)
	if er != nil || r == nil {
		log.Println(er)
		return nil, nil, nil
	}
	rr := r.(*RoleBean)
	a, er := ssql.SelectWhere(ctx, "wauth", "auth_id in("+rr.Auths+")")
	if er != nil || r == nil {
		log.Println(er)
		return nil, nil, nil
	}
	aas := make([]*AuthBean, len(a))
	for i, l := 0, len(a); i < l; i++ {
		aas[i] = a[i].(*AuthBean)
	}
	return uu, rr, aas

}

func updatetest() {
	ctx := context.Background()
	r2 := &RoleBean{
		Role_Name2: "rn22",
		Auths:      "1,2,6",
		Remark:     "rrerk",
		Role_id:    3,
	}
	ssql.Update(ctx, r2)

}

func main() {
	//ma.TAdd("dsas")
	u, r, aus := login(context.Background(), "tony2", "123")

	log.Println(u.Nick_name)
	log.Println(r.Auths)
	log.Println(len(aus))

}
